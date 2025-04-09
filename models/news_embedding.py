import pandas as pd
from tqdm import tqdm
from torch.utils.data import TensorDataset, DataLoader
import torch
from torch.amp import autocast
from transformers import AutoTokenizer
import psycopg2
import re
import psycopg2.extras
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.getcwd()[:-7])
load_dotenv()

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")

if not all([DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME]):
    print("Не все переменные окружения установлены.")
    sys.exit(1)

model_path = "matvej-melikhov-ruBERT-finetuned-lenta/mt5_fin2_0.7388_0.7722_epoch_1"
model_name_hf_hub = "matvej-melikhov/ruBERT-finetuned-lenta"

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def clean_text(text: str) -> str:
    # Заменяем управляющие символы (\n, \t и прочие) на пробел
    text = re.sub(r'\s+', ' ', text)

    # Удаляем ссылки (http, https, www)
    text = re.sub(r'https?:\/\/\S+|www\.\S+|https?:\S+', '', text)

    # Удаляем смайлики (оставляем только буквы, цифры и знаки препинания)
    text = re.sub(r'[^\w\s.,!?\"\'():;\-]', '', text)

    return text.strip()

try:
    query = f"""SELECT * FROM news"""
    with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            news = pd.DataFrame(cur.fetchall(), columns=["id", "channel", "date", "text"])
    if news.empty:
        print("Нет данных для обработки.")
        sys.exit(0)
except psycopg2.Error as e:
    print(f"Ошибка при подключении к базе данных: {e}")
    sys.exit(1)

news["text"] = news["text"].apply(clean_text)

tokenizer = AutoTokenizer.from_pretrained(model_name_hf_hub)
model = torch.load(model_path, weights_only=False)

tokens = tokenizer(news.text.tolist(), max_length=1533, truncation=True, padding=True, return_tensors='pt')

loader = DataLoader(TensorDataset(tokens["input_ids"], tokens["token_type_ids"], tokens["attention_mask"]), batch_size=50, pin_memory=True)
all_embedding = torch.empty([news.shape[0], model.bert.config.hidden_size])

index = 0
for input_ids, token_type_ids, attention_mask in tqdm(loader):
    input_ids, token_type_ids, attention_mask = input_ids.to(device), token_type_ids.to(device), attention_mask.to(device)
    if device.type == "cuda":
        with torch.no_grad(), autocast(device_type="cuda", dtype=torch.float16):
            embedding = model.bert(input_ids = input_ids, token_type_ids = token_type_ids, attention_mask = attention_mask).pooler_output
    else:
        with torch.no_grad():
            embedding = model.bert(input_ids = input_ids, token_type_ids = token_type_ids, attention_mask = attention_mask).pooler_output
    all_embedding[index: index + len(embedding)] = embedding.to("cpu")
    index += len(input_ids)

news["embedding"] = all_embedding.tolist()
news.to_csv("current.csv")
query = "INSERT INTO news_embedd VALUES %s"
with psycopg2.connect(dbname = DB_NAME, user = DB_USER, password = DB_PASS, host = DB_HOST, port = DB_PORT) as conn:
    with conn.cursor() as cur:
        data = [row for row in news[["id", "embedding"]].itertuples(index=False, name=None)]
        psycopg2.extras.execute_values(cur, query, data)