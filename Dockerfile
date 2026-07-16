# Imagen de la API para desplegar en Fargate (Spot).
FROM python:3.12-slim

# Arranque rápido y logs sin buffer (importante cuando Spot reemplaza tareas).
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE 8000

# uvicorn propaga SIGTERM a la app => se dispara el apagado ordenado (lifespan).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
