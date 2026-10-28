# -*- coding: utf-8 -*-
"""
문봇 블로그 수집 API 서버.

GitHub Actions를 수동 트리거하는 대신, 관리자 화면에서 이 서버를 바로 호출합니다.
Replit/Render/Cloud Run 같은 항상 켜진 Python 호스트에 배포하면 버튼 반응이 훨씬 빨라집니다.
"""
from __future__ import annotations

import os
import threading
import traceback
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import auth, credentials, firestore
from flask import Flask, jsonify, request

from collect import SERVICE_ACCOUNT_KEY, KST, run_collection

app = Flask(__name__)

_lock = threading.Lock()
_status = {
    "running": False,
    "startedAt": None,
    "finishedAt": None,
    "blogId": None,
    "direction": None,
    "current": 0,
    "total": 0,
    "percent": 0,
    "currentBlogId": None,
    "currentNickname": None,
    "lastError": None,
}


def _init_firebase():
    if firebase_admin._apps:
        return
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("FIREBASE_PROJECT_ID")
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or (SERVICE_ACCOUNT_KEY if os.path.exists(SERVICE_ACCOUNT_KEY) else "")
    if cred_path:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {"projectId": project_id} if project_id else None)
    else:
        firebase_admin.initialize_app(options={"projectId": project_id} if project_id else None)


def _cors_headers():
    origin = request.headers.get("Origin", "*")
    allowed = [x.strip() for x in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if x.strip()]
    allow_origin = origin if "*" in allowed or origin in allowed else allowed[0]
    return {
        "Access-Control-Allow-Origin": allow_origin,
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }


@app.after_request
def add_cors_headers(response):
    for key, value in _cors_headers().items():
        response.headers[key] = value
    return response


@app.route("/api/collect", methods=["OPTIONS"])
@app.route("/api/collect/status", methods=["OPTIONS"])
@app.route("/api/auth/reset-password", methods=["OPTIONS"])
@app.route("/api/auth/ensure-participant", methods=["OPTIONS"])
def options():
    return ("", 204)


def _require_admin():
    _init_firebase()
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return None, (jsonify({"error": "missing_token"}), 401)

    try:
        decoded = auth.verify_id_token(header.removeprefix("Bearer ").strip())
        uid = decoded["uid"]
        doc = firestore.client().collection("admins").document(uid).get()
        if not doc.exists:
            return None, (jsonify({"error": "admin_only"}), 403)
        return decoded, None
    except Exception as exc:
        return None, (jsonify({"error": "invalid_token", "message": str(exc)}), 401)


def _run_job(blog_id: str, reverse_order: bool = False):
    global _status
    def on_progress(current: int, total: int, current_blog_id: str, nickname: str):
        percent = round((current / total) * 100) if total else 0
        with _lock:
            _status.update({
                "current": current,
                "total": total,
                "percent": percent,
                "currentBlogId": current_blog_id,
                "currentNickname": nickname,
            })

    try:
        run_collection(single_id_override=blog_id, reverse_order=reverse_order, progress_callback=on_progress)
        with _lock:
            _status.update({
                "running": False,
                "finishedAt": datetime.now(KST).isoformat(),
                "percent": 100,
                "lastError": None,
            })
    except Exception as exc:
        traceback.print_exc()
        with _lock:
            _status.update({
                "running": False,
                "finishedAt": datetime.now(KST).isoformat(),
                "lastError": f"{type(exc).__name__}: {exc}",
            })


@app.post("/api/collect")
def collect_now():
    _, error = _require_admin()
    if error:
        return error

    payload = request.get_json(silent=True) or {}
    blog_id = (payload.get("blogId") or "").strip()
    direction = (payload.get("direction") or "top").strip().lower()
    reverse_order = direction in ("bottom", "reverse", "down")

    with _lock:
        if _status["running"]:
            return jsonify({"ok": False, "error": "already_running", "status": _status}), 409
        _status.update({
            "running": True,
            "startedAt": datetime.now(KST).isoformat(),
            "finishedAt": None,
            "blogId": blog_id or None,
            "direction": "bottom" if reverse_order else "top",
            "current": 0,
            "total": 0,
            "percent": 0,
            "currentBlogId": None,
            "currentNickname": None,
            "lastError": None,
        })

    thread = threading.Thread(target=_run_job, args=(blog_id, reverse_order), daemon=True)
    thread.start()
    return jsonify({"ok": True, "mode": "single" if blog_id else "all", "direction": _status["direction"], "status": _status}), 202


@app.get("/api/collect/status")
def collect_status():
    _, error = _require_admin()
    if error:
        return error
    return jsonify({"ok": True, "status": _status})


@app.post("/api/auth/reset-password")
def reset_password():
    _, error = _require_admin()
    if error:
        return error

    payload = request.get_json(silent=True) or {}
    blog_id = (payload.get("blogId") or "").strip().lower()
    uid = (payload.get("uid") or "").strip()
    password = payload.get("password") or "123456"

    if len(password) < 6:
        return jsonify({"ok": False, "error": "weak_password"}), 400
    if not uid and not blog_id:
        return jsonify({"ok": False, "error": "missing_target"}), 400

    try:
        if uid:
            user = auth.update_user(uid, password=password)
        else:
            user_record = auth.get_user_by_email(f"{blog_id}@moonsaboo.local")
            user = auth.update_user(user_record.uid, password=password)
        return jsonify({"ok": True, "uid": user.uid, "password": password})
    except auth.UserNotFoundError:
        return jsonify({"ok": False, "error": "user_not_found"}), 404
    except Exception as exc:
        return jsonify({"ok": False, "error": "reset_failed", "message": str(exc)}), 500


@app.post("/api/auth/ensure-participant")
def ensure_participant_account():
    decoded, error = _require_admin()
    if error:
        return error

    payload = request.get_json(silent=True) or {}
    blog_id = (payload.get("blogId") or "").strip().lower()
    nickname = (payload.get("nickname") or blog_id).strip()
    challenge_id = (payload.get("challengeId") or "").strip()
    password = payload.get("password") or "123456"

    if not blog_id:
        return jsonify({"ok": False, "error": "missing_blog_id"}), 400
    if len(password) < 6:
        return jsonify({"ok": False, "error": "weak_password"}), 400

    email = f"{blog_id}@moonsaboo.local"
    created = False
    try:
        user = auth.get_user_by_email(email)
        auth.update_user(user.uid, password=password, disabled=False)
    except auth.UserNotFoundError:
        user = auth.create_user(
            email=email,
            password=password,
            display_name=nickname or blog_id,
            disabled=False,
        )
        created = True

    db = firestore.client()
    db.collection("participant_users").document(user.uid).set({
        "email": email,
        "loginId": blog_id,
        "nickname": nickname or blog_id,
        "blogId": blog_id,
        "status": "approved",
        "approvedAt": firestore.SERVER_TIMESTAMP,
        "approvedBy": decoded["uid"],
        "initialPassword": True,
        "challengeId": challenge_id,
    }, merge=True)

    db.collection("participant_requests").document(user.uid).set({
        "email": email,
        "loginId": blog_id,
        "nickname": nickname or blog_id,
        "blogId": blog_id,
        "status": "approved",
        "approvedAt": firestore.SERVER_TIMESTAMP,
        "approvedBy": decoded["uid"],
        "createdByAdmin": True,
        "challengeId": challenge_id,
    }, merge=True)

    return jsonify({"ok": True, "uid": user.uid, "email": email, "created": created, "password": password})


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "time": datetime.now(timezone.utc).isoformat()})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
