FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
# Prebuilt CPU wheels from the maintainer's index avoid a 10-minute C++
# compile. If this index ever 404s: apt-get install build-essential cmake,
# drop the extra-index-url, and let pip compile from source.
RUN pip install --no-cache-dir -r requirements.txt \
    --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu
COPY server.py config.py engine.py backend_llama.py kvcache.py chatml.py ./
EXPOSE 9000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "9000"]
