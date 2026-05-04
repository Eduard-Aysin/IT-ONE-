"""
Модуль для отправки уведомлений в Telegram (через requests, без доп. библиотек)
"""
import time
import typing as tp
import requests
from loguru import logger
from airflow.models import Variable


class MessageTemplate:
    def __init__(self, message_template: str, responsible_users: tp.List[str] = []):
        self._message_template = message_template
        self._responsible_users = responsible_users

    def create_message_template(self, context: tp.Dict[str, tp.Any]) -> str:
        args = self._parse_context(context)
        return self._message_template.format(**args)

    def _parse_context(self, context: tp.Dict[str, tp.Any]) -> tp.Dict[str, tp.Any]:
        return {
            "DAG_NAME":     context.get("dag").dag_id,
            "TASK_ID":      context.get("task_instance").task_id,
            "DATE":         self._fmt_date(context.get("execution_date")),
            "TASK_LOG_URL": context.get("ti").log_url,
            "USERS":        self._users_string(),
        }

    def _fmt_date(self, date) -> str:
        return date.strftime("%Y-%m-%d %H:%M:%S") if date else ""

    def _users_string(self) -> str:
        return ", ".join([f"@{u}" for u in self._responsible_users])


class TelegramNotification:
    """
    intervals — паузы (в секундах) между попытками если не получается отправить
    """
    def __init__(
        self,
        message_template: str,
        responsible_users: tp.List[str] = [],
        intervals: tp.List[int] = [1, 60, 600],
    ):
        self._message_template = MessageTemplate(message_template, responsible_users)
        self._intervals = intervals

    def send(self, context: tp.Dict[str, tp.Any]) -> None:
        token   = Variable.get("TELEGRAM_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")
        message = self._message_template.create_message_template(context)
        url = f"https://api.telegram.org/bot{token}/sendMessage"

        for interval in self._intervals:
            try:
                response = requests.post(url, json={
                    "chat_id": chat_id,
                    "text": message,
                })
                response.raise_for_status()
                logger.info("Telegram уведомление отправлено")
                break
            except Exception as e:
                logger.info(f"Ошибка отправки в Telegram: {e}")
                time.sleep(interval)