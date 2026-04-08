"""Entry point — run with: uvicorn main:app --reload"""
from ingestion.api.app import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
