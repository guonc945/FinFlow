# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Dict, List, Optional
import uuid

class SyncProgressTracker:
    def __init__(self):
        # Dict mapping task_id -> status_dict
        self._tasks: Dict[str, dict] = {}

    def create_task(self, community_ids: List[int]) -> str:
        task_id = str(uuid.uuid4())
        self._tasks[task_id] = {
            "task_id": task_id,
            "status": "pending", # pending, running, completed, partially_completed, failed
            "total_communities": len(community_ids),
            "current_community_index": 0,
            "current_community_name": "",
            "logs": [],
            "start_time": datetime.now().isoformat(),
            "end_time": None
        }
        return task_id

    def update_status(self, task_id: str, status: str):
        if task_id in self._tasks:
            self._tasks[task_id]["status"] = status
            if status in ["completed", "failed", "partially_completed"]:
                self._tasks[task_id]["end_time"] = datetime.now().isoformat()

    def update_progress(self, task_id: str, current_index: int, community_name: str = ""):
        if task_id in self._tasks:
            self._tasks[task_id]["current_community_index"] = current_index
            if community_name:
                self._tasks[task_id]["current_community_name"] = community_name

    def add_log(self, task_id: str, message: str, log_type: str = "info"):
        if task_id in self._tasks:
            self._tasks[task_id]["logs"].append({
                "message": message,
                "type": log_type,
                "time": datetime.now().strftime("%H:%M:%S")
            })

    def get_task_status(self, task_id: str) -> Optional[dict]:
        return self._tasks.get(task_id)

    def cleanup_old_tasks(self, max_tasks: int = 100):
        if len(self._tasks) > max_tasks:
            # Simple cleanup: remove the oldest ones
            sorted_keys = sorted(self._tasks.keys(), key=lambda x: self._tasks[x]["start_time"])
            for i in range(len(sorted_keys) - max_tasks):
                del self._tasks[sorted_keys[i]]

# Singleton instance
tracker = SyncProgressTracker()
