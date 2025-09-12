import os
from datetime import date as Date
from typing import Any, Dict

import requests
from flask import Flask, render_template, request, redirect, url_for, flash


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

    api_base_url = os.environ.get("API_BASE_URL", "http://localhost:8000")

    def api_get(path: str, params: Dict[str, Any] | None = None):
        try:
            resp = requests.get(f"{api_base_url}{path}", params=params, timeout=10)
            resp.raise_for_status()
            return resp.json(), None
        except requests.exceptions.RequestException as exc:
            return None, str(exc)

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


