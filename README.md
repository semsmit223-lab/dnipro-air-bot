# dnipro-air-bot

Telegram-бот моніторингу повітряних тривог і загроз для Дніпра, Царичанки
та інших міст.

## Запуск

```bash
pip install -r requirements.txt
python dnipro_air_monitor_bot.py
```

## Змінні оточення

| Змінна | Призначення |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | Токен бота (обовʼязково) |
| `UKRAINEALARM_TOKEN` | Токен api.ukrainealarm.com для стану тривоги в місті |
| `BOT_ADMIN_IDS` | Telegram ID адміністраторів бота (через кому або пробіл) |
| `BOT_DATA_DIR` | Каталог для стану, налаштувань і медіа (типово — каталог коду) |
| `BOT_ANALYTICS_CHAT_ID` | Чат, для якого ведеться добова аналітика |
| `PORT` | Якщо задано — піднімає health endpoint `/healthz` |

Службові команди (`/test`, `/testday`, `/testdaystats`, `/testcard`,
`/testmapa`, `/chek`) доступні лише ID зі списку `BOT_ADMIN_IDS`.
