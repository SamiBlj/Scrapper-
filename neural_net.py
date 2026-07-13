"""
neural_net.py — Field-classification neural network

Two tasks handled by a single model:
  1.  PRODUCT FIELD DETECTION — given a text snippet from a web page, predict
      which product field it contains (price, name, sku, availability, …).
  2.  EXCEL COLUMN MAPPING — given a column header + sample cell values, predict
      which product field that column represents.

Architecture
------------
Input  : TF-IDF feature vector (word n-grams + char n-grams combined)
Hidden : 2 fully-connected ReLU layers  (256 → 128)
Output : softmax over 9 field classes

Implemented with scikit-learn MLPClassifier so there is no GPU/PyTorch
requirement, but the forward pass, backprop, and weight initialisation are
all standard neural-net operations happening under the hood.

All training progress, prediction details, and confidence scores are printed
to the terminal so you can watch the model learn in real time.

Usage
-----
# From server.py / any module:
from neural_net import FieldNet

net = FieldNet()
net.train()           # train on built-in labelled examples + print loss curve
net.predict("29.99 DH")           # → {'field': 'price', 'confidence': 0.97}
net.map_excel_columns(df)         # → {'ColA': 'name', 'ColB': 'price', …}
net.save()  /  net.load()         # persist to disk

# Standalone:
python neural_net.py
"""
from __future__ import annotations

import os
import pickle
import time
from typing import Optional

import numpy as np

# ── Optional coloured output ──────────────────────────────────────────────────
_C = {
    "reset": "\033[0m", "bold": "\033[1m",
    "green": "\033[92m", "red":  "\033[91m",
    "yellow":"\033[93m", "cyan": "\033[96m",
    "grey":  "\033[90m", "blue": "\033[94m",
}
def _t(msg, c="reset"): return f"{_C.get(c,'')}{msg}{_C['reset']}"
def _bar(v, width=20, colour="green"):
    filled = round(v * width)
    return _t("█" * filled + "░" * (width - filled), colour)

# ── Field labels ──────────────────────────────────────────────────────────────
FIELDS = ["price", "name", "sku", "availability", "brand", "rating", "description", "category", "irrelevant"]
FIELD_IDX = {f: i for i, f in enumerate(FIELDS)}

# ── Training corpus ───────────────────────────────────────────────────────────
# Each entry: (text_example, field_label)
# These cover the patterns the model needs to recognise on real product pages
# and in Excel column headers + sample values.

TRAINING_DATA: list[tuple[str, str]] = [
    # ── price ────────────────────────────────────────────────────────────────
    ("29.99", "price"),
    ("$29.99", "price"),
    ("€ 14,50", "price"),
    ("250 DH", "price"),
    ("1 250,00 MAD", "price"),
    ("Price: 99.95", "price"),
    ("prix : 45 €", "price"),
    ("Sale price $19.00", "price"),
    ("USD 149.00", "price"),
    ("Special offer 9.99", "price"),
    ("12.000 TND", "price"),
    ("£ 8.49", "price"),
    ("3 500 DA", "price"),
    ("Regular price 79,00 €", "price"),
    ("Prix unitaire 35,90 €", "price"),
    ("price 120", "price"),
    ("unit price", "price"),
    ("Price", "price"),
    ("Prix", "price"),
    ("Selling Price", "price"),
    ("Sale Price", "price"),
    ("Cost", "price"),
    ("Amount", "price"),
    ("Tarif", "price"),
    ("34,99 €", "price"),
    ("$1,299.00", "price"),
    ("450 MAD", "price"),
    ("2 100,00 DH", "price"),
    ("Was $89.99 Now $59.99", "price"),
    ("À partir de 19,90 €", "price"),
    ("CAD 24.99", "price"),
    ("Clearance 5.00", "price"),
    ("15.500 DT", "price"),
    ("¥ 3980", "price"),
    ("Total", "price"),
    ("Unit Cost", "price"),
    ("MSRP", "price"),
    ("Prix TTC", "price"),
    ("Prix HT", "price"),
    ("List Price", "price"),
    ("Final Price", "price"),
    ("Discounted Price", "price"),
    ("Montant", "price"),
    ("Coût unitaire", "price"),

    # ── name ─────────────────────────────────────────────────────────────────
    ("Nike Air Max 90 Men's Running Shoes", "name"),
    ("iPhone 14 Pro 256GB Space Black", "name"),
    ("Samsung QLED 55\" Smart TV 2023", "name"),
    ("Dyson V11 Cordless Vacuum Cleaner", "name"),
    ("Levi's 501 Original Fit Jeans", "name"),
    ("Product Title", "name"),
    ("Nom du produit", "name"),
    ("Intitulé", "name"),
    ("Product Name", "name"),
    ("Item Name", "name"),
    ("Name", "name"),
    ("Title", "name"),
    ("Description courte", "name"),
    ("Désignation", "name"),
    ("Libellé produit", "name"),
    ("Article", "name"),
    ("Produit", "name"),
    ("Adidas Ultraboost 22 Running Shoe White Size 42", "name"),
    ("Canon EOS R5 Mirrorless Camera Body", "name"),
    ("Bosch Serie 6 Washing Machine 9kg", "name"),
    ("Sony WH-1000XM5 Wireless Noise Cancelling Headphones", "name"),
    ("Instant Pot Duo 7-in-1 Electric Pressure Cooker 6Qt", "name"),
    ("Zara Oversized Wool Blend Coat", "name"),
    ("Lenovo ThinkPad X1 Carbon Gen 11 14\"", "name"),
    ("L'Oréal Paris Revitalift Anti-Aging Cream 50ml", "name"),
    ("Nom", "name"),
    ("Appellation", "name"),
    ("Product", "name"),
    ("Item Title", "name"),
    ("Nom de l'article", "name"),
    ("Modèle", "name"),
    ("Nike Air Force 1 '07 White Sneakers", "name"),
    ("KitchenAid Artisan Stand Mixer 5-Quart", "name"),
    ("JBL Flip 6 Portable Bluetooth Speaker", "name"),

    # ── sku ──────────────────────────────────────────────────────────────────
    ("SKU: AB-12345", "sku"),
    ("REF-99021-BLK", "sku"),
    ("Model: MWP82LL/A", "sku"),
    ("Part Number: 7890-XYZ", "sku"),
    ("EAN: 5901234123457", "sku"),
    ("Barcode 0012345678905", "sku"),
    ("Item code: IC-00451", "sku"),
    ("Product ID", "sku"),
    ("SKU", "sku"),
    ("Reference", "sku"),
    ("Référence", "sku"),
    ("Code produit", "sku"),
    ("Code article", "sku"),
    ("Ref", "sku"),
    ("Barcode", "sku"),
    ("EAN", "sku"),
    ("UPC", "sku"),
    ("ASIN", "sku"),
    ("Part No", "sku"),
    ("Item #", "sku"),
    ("Numéro de modèle", "sku"),
    ("ISBN: 978-3-16-148410-0", "sku"),
    ("GTIN 00012345678905", "sku"),
    ("Ref. 4521-XG", "sku"),
    ("Modèle n° 7734-B", "sku"),
    ("Article No.: A00982", "sku"),
    ("Stock Keeping Unit", "sku"),
    ("Code-barres", "sku"),
    ("MPN", "sku"),
    ("Serial Number", "sku"),
    ("Numéro de série", "sku"),
    ("Product Code", "sku"),
    ("Code EAN13", "sku"),
    ("Variant ID", "sku"),
    ("External ID", "sku"),

    # ── availability ─────────────────────────────────────────────────────────
    ("In Stock", "availability"),
    ("Out of Stock", "availability"),
    ("Available", "availability"),
    ("Unavailable", "availability"),
    ("En stock", "availability"),
    ("Rupture de stock", "availability"),
    ("Disponible", "availability"),
    ("Limited stock — only 3 left", "availability"),
    ("Ships in 2–5 days", "availability"),
    ("Pre-order", "availability"),
    ("Back order", "availability"),
    ("Stock", "availability"),
    ("Availability", "availability"),
    ("Disponibilité", "availability"),
    ("Statut", "availability"),
    ("Status", "availability"),
    ("Inventory", "availability"),
    ("Qty", "availability"),
    ("Quantity", "availability"),
    ("Stock level", "availability"),
    ("Sold Out", "availability"),
    ("Épuisé", "availability"),
    ("En rupture", "availability"),
    ("Only 2 left in stock", "availability"),
    ("Ready to ship", "availability"),
    ("Coming Soon", "availability"),
    ("Expédié sous 24h", "availability"),
    ("Livraison immédiate", "availability"),
    ("Stock disponible", "availability"),
    ("Currently unavailable", "availability"),
    ("In stock, usually ships within 1 day", "availability"),
    ("État du stock", "availability"),
    ("Stock restant", "availability"),
    ("Delivery status", "availability"),

    # ── brand ─────────────────────────────────────────────────────────────────
    ("Nike", "brand"),
    ("Samsung", "brand"),
    ("Apple", "brand"),
    ("Bosch", "brand"),
    ("Adidas", "brand"),
    ("Brand: Sony", "brand"),
    ("Marque : Philips", "brand"),
    ("Manufacturer: Canon", "brand"),
    ("Brand", "brand"),
    ("Marque", "brand"),
    ("Fabricant", "brand"),
    ("Manufacturer", "brand"),
    ("Vendor", "brand"),
    ("Fournisseur", "brand"),
    ("Make", "brand"),
    ("Label", "brand"),
    ("Puma", "brand"),
    ("LG", "brand"),
    ("Whirlpool", "brand"),
    ("Dell", "brand"),
    ("Marque : Lenovo", "brand"),
    ("Fabricant : Bosch", "brand"),
    ("Made by Panasonic", "brand"),
    ("Enseigne", "brand"),
    ("Distributeur", "brand"),
    ("Supplier", "brand"),
    ("Marque déposée", "brand"),
    ("House brand", "brand"),

    # ── rating ────────────────────────────────────────────────────────────────
    ("4.5 out of 5", "rating"),
    ("★★★★☆ 4.2", "rating"),
    ("Rated 3/5", "rating"),
    ("9.1 / 10", "rating"),
    ("4.8 stars (230 reviews)", "rating"),
    ("Note : 4,6/5", "rating"),
    ("Rating", "rating"),
    ("Note", "rating"),
    ("Score", "rating"),
    ("Stars", "rating"),
    ("Review score", "rating"),
    ("Avis clients", "rating"),
    ("Customer rating", "rating"),
    ("Avg rating", "rating"),
    ("4.0 out of 5 stars", "rating"),
    ("★★★☆☆", "rating"),
    ("7.8/10", "rating"),
    ("Noté 4,1 sur 5", "rating"),
    ("92% recommend this product", "rating"),
    ("Customer Reviews", "rating"),
    ("Évaluation", "rating"),
    ("Note moyenne", "rating"),
    ("Overall rating", "rating"),
    ("User score", "rating"),
    ("Satisfaction", "rating"),

    # ── description ──────────────────────────────────────────────────────────
    ("Lightweight and breathable design ideal for long-distance running on any surface.", "description"),
    ("High-performance blender with 1200W motor and 6-blade stainless steel assembly.", "description"),
    ("Made from 100% organic cotton. Machine washable at 40°C. Available in 5 colours.", "description"),
    ("Description", "description"),
    ("Product description", "description"),
    ("Details", "description"),
    ("Détails", "description"),
    ("Description produit", "description"),
    ("Caractéristiques", "description"),
    ("Features", "description"),
    ("Specifications", "description"),
    ("About this item", "description"),
    ("Short description", "description"),
    ("Long description", "description"),
    ("Body", "description"),
    ("Text", "description"),
    ("Content", "description"),
    ("Notes", "description"),
    ("Waterproof up to 50 meters with scratch-resistant sapphire crystal glass.", "description"),
    ("Fabriqué en France à partir de matériaux recyclés, certifié OEKO-TEX.", "description"),
    ("Ergonomic design reduces wrist strain during extended use sessions.", "description"),
    ("Aperçu", "description"),
    ("Résumé", "description"),
    ("Product overview", "description"),
    ("Key features", "description"),
    ("Points forts", "description"),
    ("What's in the box", "description"),
    ("Contenu de l'emballage", "description"),
    ("Technical details", "description"),
    ("Fiche technique", "description"),
    ("Product story", "description"),

    # ── category ─────────────────────────────────────────────────────────────
    ("Shoes > Running", "category"),
    ("Electronics > Cameras > DSLR", "category"),
    ("Home Appliances", "category"),
    ("Category", "category"),
    ("Catégorie", "category"),
    ("Department", "category"),
    ("Section", "category"),
    ("Type", "category"),
    ("Collection", "category"),
    ("Famille", "category"),
    ("Sous-catégorie", "category"),
    ("Product type", "category"),
    ("Genre", "category"),
    ("Rayon", "category"),
    ("Kitchen > Small Appliances > Blenders", "category"),
    ("Vêtements > Femme > Manteaux", "category"),
    ("Sports & Outdoors", "category"),
    ("Rubrique", "category"),
    ("Univers", "category"),
    ("Product family", "category"),
    ("Segment", "category"),
    ("Classe", "category"),
    ("Product group", "category"),
    ("Gamme", "category"),
    ("Sub-department", "category"),

    # ── irrelevant ────────────────────────────────────────────────────────────
    ("Email address", "irrelevant"),
    ("Date added", "irrelevant"),
    ("Last modified", "irrelevant"),
    ("Internal ID", "irrelevant"),
    ("Row number", "irrelevant"),
    ("Sheet1", "irrelevant"),
    ("Column1", "irrelevant"),
    ("Unnamed: 0", "irrelevant"),
    ("Created at", "irrelevant"),
    ("Updated at", "irrelevant"),
    ("user_id", "irrelevant"),
    ("session", "irrelevant"),
    ("token", "irrelevant"),
    ("index", "irrelevant"),
    ("hash", "irrelevant"),
    ("uuid", "irrelevant"),
    ("image url", "irrelevant"),
    ("photo", "irrelevant"),
    ("thumbnail", "irrelevant"),
    ("weight", "irrelevant"),
    ("dimension", "irrelevant"),
    ("colour", "irrelevant"),
    ("size", "irrelevant"),
    ("Taille", "irrelevant"),
    ("Couleur", "irrelevant"),
    ("Poids", "irrelevant"),
    ("Warehouse location", "irrelevant"),
    ("Supplier ID", "irrelevant"),
    ("Tax rate", "irrelevant"),
    ("Currency code", "irrelevant"),
    ("Locale", "irrelevant"),
    ("IP address", "irrelevant"),
    ("Browser agent", "irrelevant"),
    ("Cache key", "irrelevant"),
    ("Slug", "irrelevant"),
    ("Meta title", "irrelevant"),
    ("Meta description", "irrelevant"),
    ("Canonical URL", "irrelevant"),
    ("Cookie consent", "irrelevant"),
    ("Referrer", "irrelevant"),
    ("Timestamp", "irrelevant"),
    ("Checksum", "irrelevant"),
    ("Language", "irrelevant"),
    ("Devise", "irrelevant"),
    ("Emplacement", "irrelevant"),
    ("Entrepôt", "irrelevant"),
]


# ── Model wrapper ─────────────────────────────────────────────────────────────

MODEL_PATH = os.path.join(os.path.dirname(__file__), "field_net.pkl")


class FieldNet:
    """
    A TF-IDF + MLP neural network for product field classification.

    The network architecture (inside scikit-learn's MLPClassifier):
      Input layer  : TF-IDF sparse vector (≈ 5 000 features)
      Hidden layer 1 : 256 neurons, ReLU activation
      Hidden layer 2 : 128 neurons, ReLU activation
      Output layer : 9 neurons, softmax (one per field class)

    Training uses Adam optimiser with early-stopping on a 15% validation split.
    """

    def __init__(self):
        self._tfidf  = None   # sklearn TfidfVectorizer
        self._model  = None   # sklearn MLPClassifier
        self._trained = False

    # ── Training ──────────────────────────────────────────────────────────────

    def train(self, extra_data: Optional[list[tuple[str, str]]] = None):
        """
        Train (or retrain) the model.
        extra_data: list of (text, field_label) pairs to augment the built-in corpus.
        """
        try:
            from sklearn.neural_network import MLPClassifier
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.model_selection import train_test_split
            from sklearn.metrics import classification_report
        except ImportError:
            print("Failed to run imports")
            print(_t("✘ scikit-learn not installed. Run: pip install scikit-learn", "red"))
            return

        corpus = list(TRAINING_DATA)
        if extra_data:
            corpus.extend(extra_data)
            print(f"  + {len(extra_data)} custom examples added to training set")

        texts  = [t for t, _ in corpus]
        labels = [l for _, l in corpus]

        print(
            f"\n{_t('═'*60,'cyan')}\n"
            f"{_t('NEURAL NET — TRAINING','bold')}\n"
            f"Corpus : {len(texts)} examples across {len(FIELDS)} field classes\n"
            f"Architecture : TF-IDF → 256 → 128 → {len(FIELDS)}\n"
            f"{_t('═'*60,'cyan')}"
        )

        # ── Feature extraction ────────────────────────────────────────────────
        print(f"\n{_t('Step 1','blue')} Building TF-IDF vocabulary…")
        self._tfidf = TfidfVectorizer(
            analyzer="char_wb",   # character n-grams — great for short strings
            ngram_range=(2, 4),   # bigrams through 4-grams
            max_features=8_000,
            sublinear_tf=True,
        )
        # Stack a second vectorizer on word n-grams and concatenate
        from sklearn.pipeline import FeatureUnion
        from sklearn.feature_extraction.text import TfidfVectorizer as TV
        from scipy.sparse import hstack

        word_vec  = TV(analyzer="word",    ngram_range=(1,2), max_features=4_000, sublinear_tf=True)
        char_vec  = TV(analyzer="char_wb", ngram_range=(2,4), max_features=4_000, sublinear_tf=True)

        X_word = word_vec.fit_transform(texts)
        X_char = char_vec.fit_transform(texts)
        X      = hstack([X_word, X_char])

        self._tfidf = (word_vec, char_vec)   # keep both for inference

        y = np.array([FIELD_IDX[l] for l in labels])

        print(f"  Feature matrix: {X.shape[0]} samples × {X.shape[1]} features")

        # ── Train / val split ─────────────────────────────────────────────────
        X_tr, X_val, y_tr, y_val = train_test_split(X, y, test_size=0.15, stratify=y, random_state=42)
        print(f"  Train: {X_tr.shape[0]}  Val: {X_val.shape[0]}")

        # ── Network ───────────────────────────────────────────────────────────
        print(f"\n{_t('Step 2','blue')} Training MLP (Adam, max 1000 epochs, early stopping)…\n")
        self._model = MLPClassifier(
            hidden_layer_sizes=(256, 128),
            activation="relu",
            solver="adam",
            alpha=1e-4,           # L2 regularisation
            batch_size="auto",
            learning_rate="adaptive",
            max_iter=1000,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=20,
            verbose=True,        # we print our own progress
            random_state=42,
        )

        t0 = time.time()
        self._model.fit(X_tr, y_tr)
        elapsed = round(time.time() - t0, 2)

        # ── Loss curve ────────────────────────────────────────────────────────
        losses  = self._model.loss_curve_
        val_scores = self._model.validation_scores_
        n_iter  = len(losses)
        print(f"  Converged after {_t(str(n_iter),'yellow')} epochs in {elapsed}s\n")
        print(f"  {'Epoch':>6}  {'Train Loss':>11}  {'Val Acc':>8}  Graph")
        step = max(1, n_iter // 10)
        for i in range(0, n_iter, step):
            loss = losses[i]
            acc  = val_scores[i] if i < len(val_scores) else float("nan")
            bar  = _bar(acc)
            print(f"  {i+1:>6}  {loss:>11.4f}  {acc:>7.1%}  {bar}")
        # Final line
        print(f"  {n_iter:>6}  {losses[-1]:>11.4f}  {val_scores[-1]:>7.1%}  {_bar(val_scores[-1])}")

        # ── Validation report ─────────────────────────────────────────────────
        print(f"\n{_t('Step 3','blue')} Validation classification report:\n")
        y_pred = self._model.predict(X_val)
        report = classification_report(y_val, y_pred, target_names=FIELDS, zero_division=0)
        print(report)

        self._trained = True
        print(f"{_t('✔ Training complete','green')}  final val accuracy = {_t(f'{val_scores[-1]:.1%}','bold')}\n")

    # ── Inference ─────────────────────────────────────────────────────────────

    def _transform(self, texts: list[str]):
        from scipy.sparse import hstack
        word_vec, char_vec = self._tfidf
        return hstack([word_vec.transform(texts), char_vec.transform(texts)])

    def predict(self, text: str, verbose: bool = True) -> dict:
        """
        Predict which product field a text snippet belongs to.
        Returns {"field": str, "confidence": float, "all_scores": dict}
        """
        if not self._trained:
            print(_t("⚠  Model not trained — run net.train() first", "yellow"))
            return {}

        X = self._transform([text])
        proba   = self._model.predict_proba(X)[0]
        idx     = int(np.argmax(proba))
        field   = FIELDS[idx]
        conf    = float(proba[idx])
        scores  = {FIELDS[i]: round(float(p), 4) for i, p in enumerate(proba)}

        if verbose:
            print(f"\n{_t('PREDICT','bold')} {_t(repr(text),'cyan')}")
            print(f"  → {_t(field,'green')}  confidence={_t(f'{conf:.1%}','bold')}")
            # Show top-3 alternatives
            top3 = sorted(scores.items(), key=lambda x: -x[1])[:3]
            for f, p in top3:
                bar = _bar(p, width=15, colour="green" if f == field else "grey")
                print(f"  {f:<14} {bar} {p:.1%}")

        return {"field": field, "confidence": conf, "all_scores": scores}

    def predict_page_fields(self, candidates: list[dict], verbose: bool = True) -> list[dict]:
        """
        Classify a list of text candidates scraped from a product page.
        Each candidate: {"text": str, "selector": str (optional)}
        Returns the same list with "field" and "confidence" added.

        This is the neural-net path used instead of hardcoded regex patterns.
        Only candidates with confidence ≥ 0.60 are kept.
        """
        if not self._trained:
            print(_t("⚠  Model not trained", "yellow"))
            return candidates

        texts = [c["text"] for c in candidates]
        if not texts:
            return candidates

        X     = self._transform(texts)
        proba = self._model.predict_proba(X)

        results = []
        if verbose:
            print(f"\n{_t('PAGE FIELD SCAN','bold')} — {len(texts)} candidates")
            print(f"  {'Field':<14} {'Conf':>6}  {'Text preview'}")

        for i, (cand, prob) in enumerate(zip(candidates, proba)):
            idx   = int(np.argmax(prob))
            field = FIELDS[idx]
            conf  = float(prob[idx])
            if field == "irrelevant" or conf < 0.55:
                continue
            enriched = {**cand, "field": field, "confidence": conf}
            results.append(enriched)
            if verbose:
                preview = cand["text"][:50].replace("\n", " ")
                print(f"  {_t(field,'green'):<14} {conf:>6.1%}  {preview}")

        if verbose:
            print(f"  → {len(results)} high-confidence field(s) found")
        return results

    # ── Excel column mapping ──────────────────────────────────────────────────

    def map_excel_columns(self, df, verbose: bool = True) -> dict[str, str]:
        """
        Given a pandas DataFrame, predict which product field each column
        represents by combining the column header with a sample of the values.

        Returns {"column_name": "field_label"} for every column.
        Columns mapped to "irrelevant" are still included so you can see them.
        """
        if not self._trained:
            print(_t("⚠  Model not trained — run net.train() first", "yellow"))
            return {}

        print(f"\n{_t('═'*60,'cyan')}")
        print(f"{_t('EXCEL COLUMN MAPPER','bold')} — {len(df.columns)} columns, {len(df)} rows")
        print(f"{_t('═'*60,'cyan')}\n")

        mapping: dict[str, str] = {}
        for col in df.columns:
            # Build a rich text string: "header: val1, val2, val3"
            samples = df[col].dropna().astype(str).head(5).tolist()
            combined = f"{col}: " + ", ".join(samples)

            result = self.predict(combined, verbose=False)
            field  = result.get("field", "irrelevant")
            conf   = result.get("confidence", 0.0)
            mapping[col] = field

            if verbose:
                colour  = "green" if field != "irrelevant" else "grey"
                bar     = _bar(conf, width=12, colour=colour)
                sample_preview = (", ".join(samples))[:45]
                print(
                    f"  {_t(col,'bold'):<25}  {_t(field,colour):<14}  {bar} {conf:.0%}"
                    f"\n    samples: {_t(sample_preview,'grey')}"
                )

        # Summary
        mapped = {c: f for c, f in mapping.items() if f != "irrelevant"}
        print(f"\n{_t('Mapping result','bold')} — {len(mapped)}/{len(df.columns)} columns identified:\n")
        for col, field in mapping.items():
            icon = _t("✔","green") if field != "irrelevant" else _t("–","grey")
            print(f"  {icon}  {col:<25} → {_t(field,'cyan' if field!='irrelevant' else 'grey')}")

        return mapping

    # ── Persist ───────────────────────────────────────────────────────────────

    def save(self, path: str = MODEL_PATH):
        with open(path, "wb") as f:
            pickle.dump({"tfidf": self._tfidf, "model": self._model}, f)
        print(f"{_t('✔ Model saved','green')} → {path}")

    def load(self, path: str = MODEL_PATH) -> bool:
        if not os.path.exists(path):
            return False
        with open(path, "rb") as f:
            data = pickle.load(f)
        self._tfidf   = data["tfidf"]
        self._model   = data["model"]
        self._trained = True
        print(f"{_t('✔ Model loaded','green')} ← {path}")
        return True 


# ── Singleton used by server ──────────────────────────────────────────────────

_net: Optional[FieldNet] = None

def get_net() -> FieldNet:
    """Return the singleton FieldNet, loading or training if needed."""
    global _net
    if _net is None:
        _net = FieldNet()
        if not _net.load():
            print(_t("No saved model found — training now…", "yellow"))
            _net.train()
            _net.save()
    return _net


# ── CLI — run `python neural_net.py` to train and demo ───────────────────────

if __name__ == "__main__":
    print(_t("\n  eProgram Neural Field Classifier\n", "bold"))

    net = FieldNet()
    net.train()
    net.save()

    print(f"\n{_t('═'*60,'cyan')}")
    print(f"{_t('DEMO PREDICTIONS','bold')}\n")

    samples = [
        "29.99 DH",
        "iPhone 14 Pro 128GB Midnight",
        "SKU: NKE-AM90-BLK-42",
        "In Stock",
        "Nike",
        "4.7 / 5 stars",
        "Breathable mesh upper for maximum airflow during long runs",
        "Shoes > Running > Road",
        "user_id",
        "Prix : 450,00 MAD",
        "Référence produit",
        "Disponible",
    ]

    for s in samples:
        net.predict(s)
        print()

    # Demo Excel mapping
    try:
        import pandas as pd
        demo_df = pd.DataFrame({
            "Product Name":   ["Nike Air Max", "Adidas Stan Smith"],
            "Price":          ["29.99", "24.50"],
            "SKU":            ["NKE-001", "ADI-002"],
            "Brand":          ["Nike", "Adidas"],
            "In Stock?":      ["Yes", "No"],
            "Customer Score": ["4.5", "4.2"],
            "Details":        ["Running shoe", "Classic sneaker"],
            "Category":       ["Shoes", "Shoes"],
            "row_id":         ["1", "2"],
        })
        net.map_excel_columns(demo_df)
    except ImportError:
        print(_t("(pandas not installed — skipping Excel demo)", "grey"))
