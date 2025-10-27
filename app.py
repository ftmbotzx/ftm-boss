
"""
Web application to display BKNMU notifications
Runs on port 5000 and shows notifications with translations
"""

import asyncio
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify
from database import Database
from translator import Translator

logger = logging.getLogger(__name__)

app = Flask(__name__)
db = Database()
translator = Translator()

@app.route('/')
def home():
    """Display home page with notifications"""
    return render_template('index.html')

@app.route('/api/notifications')
def get_notifications():
    """API endpoint to get recent notifications"""
    try:
        # Get recent notifications from database
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        notifications = loop.run_until_complete(db.get_recent_notifications(limit=50))
        loop.close()

        # Format notifications for display
        formatted_notifications = []
        for notif in notifications:
            formatted_notifications.append({
                'id': notif.get('id'),
                'title': notif.get('title'),
                'title_en': notif.get('title_en'),
                'type': notif.get('type'),
                'date': notif.get('date'),
                'processed_at': notif.get('processed_at')
            })

        return jsonify({
            'success': True,
            'count': len(formatted_notifications),
            'notifications': formatted_notifications
        })
    except Exception as e:
        logger.error(f"Error fetching notifications: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/translate/<notification_id>')
def translate_notification(notification_id):
    """API endpoint to translate a specific notification"""
    try:
        # This would fetch from database and translate
        # For now, return a placeholder
        return jsonify({
            'success': True,
            'notification_id': notification_id,
            'translation': 'Translation service ready'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def initialize_web_app():
    """Initialize the web application"""
    logger.info("Initializing web application...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(db.initialize())
    loop.close()
    logger.info("Web application initialized successfully")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    initialize_web_app()
    app.run(host='0.0.0.0', port=5000, debug=False)
