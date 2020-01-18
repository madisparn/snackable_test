Setup with:
python3 -m venv venv
source venv/bin/activate
pip install httpx starlette uvicorn
pip freeze

Run server:
uvicorn --port 5000 src.proxy.api:app