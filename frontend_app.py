import os
from datetime import date as Date
from typing import Any, Dict, List

import requests
from flask import Flask, render_template, request, redirect, url_for, flash
import time
import uuid
from .dags.utils import get_logger


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

    api_base_url = os.environ.get("API_BASE_URL", "http://localhost:8000")
    log = get_logger(__name__, log_file="./logs/frontend.log")

    def api_get(path: str, params: Dict[str, Any] | None = None):
        start = time.time()
        url = f"{api_base_url}{path}"
        corr_id = str(uuid.uuid4())
        try:
            resp = requests.get(url, params=params, timeout=10, headers={"x-correlation-id": corr_id})
            dur = int((time.time() - start) * 1000)
            resp.raise_for_status()
            log.info(f"GET {url} -> {resp.status_code} in {dur}ms")
            return resp.json(), None
        except requests.exceptions.RequestException as exc:
            dur = int((time.time() - start) * 1000)
            log.error(f"GET {url} failed in {dur}ms: {exc}")
            return None, str(exc)

    @app.before_request
    def _start_request_logging():
        request._start_time = time.time()
        request._corr_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))

    @app.after_request
    def _finish_request_logging(response):
        start = getattr(request, "_start_time", time.time())
        dur = int((time.time() - start) * 1000)
        log.info(f"{request.method} {request.path} -> {response.status_code} in {dur}ms")
        response.headers["x-correlation-id"] = getattr(request, "_corr_id", "")
        return response

    @app.get("/")
    def index():
        today = Date.today().isoformat()
        return redirect(url_for("dashboard", date=today))

    @app.get("/dashboard")
    def dashboard():
        date_str = request.args.get("date") or Date.today().isoformat()

        sales, err1 = api_get("/sales/daily", {"date": date_str})
        if err1:
            flash(f"Failed to load daily sales: {err1}", "error")
            sales = None

        top_books, err2 = api_get("/books/top", {"limit": 5})
        if err2:
            flash(f"Failed to load top books: {err2}", "error")
            top_books = []

        return render_template("dashboard.html", sales=sales, top_books=top_books, date_str=date_str)

    @app.get("/users/<int:user_id>/purchases")
    def user_purchases(user_id: int):
        limit = int(request.args.get("limit", 50))
        purchases, err = api_get(f"/users/{user_id}/purchases", {"limit": limit})
        if err:
            flash(f"Failed to load purchases: {err}", "error")
            purchases = []
        return render_template("purchases.html", user_id=user_id, purchases=purchases, limit=limit)

    return app


app = create_app()


