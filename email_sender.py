import yagmail

def send_email(missing_stocks, message):
    yag = yagmail.SMTP("sender_email", "app_password")

    if missing_stocks.length > 0:
        body_text = f"The following stock names were not found in the database:\n\n" + "\n".join(missing_stocks) + message
    else:
        body_text = message

    yag.send(to="", subject="Data upload status", contents=body_text)