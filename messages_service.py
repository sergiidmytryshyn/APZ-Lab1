from fastapi import FastAPI

app = FastAPI()

@app.get("/message")
async def get_message():
    return "not implemented yet"
