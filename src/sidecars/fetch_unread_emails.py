import os
import email
from email.header import decode_header
from imapclient import IMAPClient
import json

env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../bounty_operations/.agents/.env'))
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

def extract_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                return part.get_payload(decode=True).decode()
    else:
        return msg.get_payload(decode=True).decode()
    return ""

def main():
    unread_emails = []
    try:
        with IMAPClient('imap.gmail.com') as client:
            client.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            client.select_folder('INBOX')
            messages = client.search('UNSEEN')
            
            for uid, message_data in client.fetch(messages, 'RFC822').items():
                email_message = email.message_from_bytes(message_data[b'RFC822'])
                subject, encoding = decode_header(email_message["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else 'utf-8')
                
                body = extract_text(email_message)
                
                # Clean up quotes
                lines = []
                for line in body.split('\n'):
                    if line.startswith('>') or line.startswith('On ') or 'wrote:' in line:
                        continue
                    lines.append(line)
                clean_body = '\n'.join(lines).strip()
                if not clean_body:
                    clean_body = body
                    
                is_alert = subject.startswith("[Bounty Engine ALERT]")
                
                if not is_alert:
                    unread_emails.append({"subject": subject, "body": clean_body})
                client.add_flags(uid, '\\Seen')
                
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        return

    print(json.dumps(unread_emails))

if __name__ == "__main__":
    main()
