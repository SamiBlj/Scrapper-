from ollama import chat
from ollama import ChatResponse
import re
from bs4 import BeautifulSoup

# HTML Parser


html_data = "<div><span>Item Total:</span><span>$4544</span></div>"



# Extract the numeric context from HTML before sending to the LLM.
def extract_number_contexts(html_content, radius=60):
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(" ", strip=True)
    contexts = []
    for match in re.finditer(r"[\d][\d\.,]*", text):
        start = max(0, match.start() - radius)
        end = min(len(text), match.end() + radius)
        context_text = text[start:end].strip()
        contexts.append({
            "number": match.group(),
            "context": context_text,
        })
    return contexts


def build_prompt(html_content):
    contexts = extract_number_contexts(html_content)
    if not contexts:
        return "No numeric contexts were found in the HTML data."

    lines = [f"Number: {c['number']} | Context: {c['context']}" for c in contexts]
    return "\n".join(lines)


prompt = build_prompt(html_data)

print(prompt)

response: ChatResponse = chat(
    model='gemma3', 
    messages=[
        {
            'role': 'user',
            'content': prompt,
        },
    ]
)

print(response['message']['content'])



'''
get ollama to go through the entire html code and find specific items
Might take a lot of time, but currently best option.
'''

