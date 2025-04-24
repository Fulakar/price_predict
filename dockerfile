FROM apache/airflow:2.9.3-python3.11

# Копируем requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt
