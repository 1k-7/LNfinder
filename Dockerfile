FROM python:3.10-slim

WORKDIR /app

# Dependencies for ebooklib (lxml) and compilation
RUN apt-get update && apt-get install -y \
gcc \
libxml2-dev \
libxslt-dev \
zlib1g-dev \
&& rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
telethon \
motor \
ebooklib \
beautifulsoup4 \
lxml

# Copy all scripts
COPY . .

CMD ["python", "bot.py"]