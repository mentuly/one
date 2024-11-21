from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import uuid, os, time, csv, json, uvicorn

app = FastAPI()

UPLOAD_DIR = "uploads"
PROCESSED_DIR = "processed"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

tasks = {}

def process_file(file_path: str, task_id: str):
    try:
        time.sleep(5)
        processed_file_path = os.path.join(PROCESSED_DIR, f"{task_id}_processed.json")

        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.endswith(".csv"):
                data = list(csv.DictReader(f))
            elif file_path.endswith(".json"):
                data = json.load(f)
            else:
                raise ValueError("Непідтримуваний формат файлу")

        with open(processed_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        tasks[task_id] = {"status": "completed", "result": processed_file_path}
    except Exception as e:
        tasks[task_id] = {"status": "failed", "error": str(e)}

@app.post("/upload/")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, file.filename)

    with open(file_path, "wb") as f:
        f.write(await file.read())

    tasks[task_id] = {"status": "processing"}
    background_tasks.add_task(process_file, file_path, task_id)

    return {"task_id": task_id, "message": "File uploaded successfully"}

@app.get("/status/")
def get_status(task_id: str):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.get("/download/")
def download_file(task_id: str):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="Task not found")
    return FileResponse(task["result"], filename=f"{task_id}_processed.json")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)