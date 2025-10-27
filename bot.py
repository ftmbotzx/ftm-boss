#!/usr/bin/env python3
"""
FTM Boss Assistant - BKNMU Notification Scraper Bot
Main entry point for the bot that scrapes BKNMU notifications and sends to Telegram
"""

import asyncio
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timedelta
from typing import Dict, List
import aiohttp
import aiofiles
import json

from config import Config
from database import Database
from scraper import BKNMUScraper
from translator import Translator


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and enhanced styling"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RED': '\033[31m',        # Red (for separator)
        'RESET': '\033[0m',       # Reset
        'BOLD': '\033[1m',        # Bold
        'DIM': '\033[2m',         # Dim
        'UNDERLINE': '\033[4m',   # Underline
    }

    # Icons for different log levels
    ICONS = {
        'DEBUG': '🔍',
        'INFO': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🚨',
    }

    def format(self, record):
        # Get color and icon for log level
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        icon = self.ICONS.get(record.levelname, '📝')

        # Format timestamp with styling
        timestamp = f"{self.COLORS['DIM']}{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}{self.COLORS['RESET']}"

        # Format logger name with styling
        logger_name = f"{self.COLORS['BOLD']}{record.name}{self.COLORS['RESET']}"

        # Format level with color and icon
        level = f"{color}{self.COLORS['BOLD']}{icon} {record.levelname:<8}{self.COLORS['RESET']}"

        # Format message
        message = record.getMessage()

        # Special formatting for different message types
        if record.levelname == 'ERROR':
            message = f"{self.COLORS['ERROR']}{self.COLORS['BOLD']}{message}{self.COLORS['RESET']}"
        elif record.levelname == 'WARNING':
            message = f"{self.COLORS['WARNING']}{message}{self.COLORS['RESET']}"
        elif record.levelname == 'INFO':
            if 'successfully' in message.lower() or 'completed' in message.lower():
                message = f"{self.COLORS['INFO']}{self.COLORS['BOLD']}{message}{self.COLORS['RESET']}"
            else:
                message = f"{self.COLORS['INFO']}{message}{self.COLORS['RESET']}"
        elif record.levelname == 'DEBUG':
            message = f"{self.COLORS['DEBUG']}{self.COLORS['DIM']}{message}{self.COLORS['RESET']}"

        # Create separator line for better readability
        if record.levelname in ['ERROR', 'CRITICAL']:
            separator = f"{self.COLORS['RED']}{'='*80}{self.COLORS['RESET']}"
            formatted_message = f"{separator}\n{timestamp} | {logger_name} | {level} | {message}\n{separator}"
        else:
            formatted_message = f"{timestamp} | {logger_name} | {level} | {message}"

        return formatted_message


class FileFormatter(logging.Formatter):
    """Formatter for file output without ANSI codes"""

    def format(self, record):
        timestamp = self.formatTime(record, '%Y-%m-%d %H:%M:%S')
        return f"{timestamp} | {record.name} | {record.levelname:<8} | {record.getMessage()}"


def setup_professional_logging():
    """Setup professional colored logging system"""

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter()
    console_handler.setFormatter(console_formatter)

    # File handler without colors
    file_handler = logging.FileHandler('ftm_boss_assistant.log', mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = FileFormatter()
    file_handler.setFormatter(file_formatter)

    # Add handlers to root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Create startup banner
    print(f"""
{ColoredFormatter.COLORS['BOLD']}{ColoredFormatter.COLORS['INFO']}
╔══════════════════════════════════════════════════════════════════════════════╗
║                        🤖 FTM BOSS ASSISTANT v2.0                           ║
║                     Professional BKNMU Notification Bot                     ║
║                                                                              ║
║  🚀 Features: Real-time Monitoring | Smart Translation | Database Storage   ║
║  📊 Status: Advanced Logging System Enabled                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
{ColoredFormatter.COLORS['RESET']}""")


# Initialize professional logging
setup_professional_logging()
logger = logging.getLogger(__name__)


class FTMBossAssistant:
    """Main bot class for FTM Boss Assistant"""

    def __init__(self):
        self.config = Config()
        self.database = Database()
        self.scraper = BKNMUScraper()
        self.translator = Translator()
        self.session = aiohttp.ClientSession()
        self.telegram_api_url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}"
        self.running = False
        self._last_update_id = 0  # Track last processed update to avoid duplicates

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False

    async def initialize(self):
        """Initialize the bot and database"""
        logger.info("🔧 Initializing FTM Boss Assistant components...")

        # Initialize database
        logger.info("🗄️  Setting up database connection...")
        await self.database.initialize()
        logger.info("✅ Database initialized and ready")

        # Test Telegram bot connection
        try:
            logger.info("🔗 Testing Telegram Bot API connection...")
            async with self.session.get(f"{self.telegram_api_url}/getMe") as response:
                if response.status == 200:
                    bot_info = await response.json()
                    logger.info(f"🤖 Bot connected successfully: @{bot_info['result']['username']}")
                    logger.info(f"📋 Bot ID: {bot_info['result']['id']}")
                    logger.info(f"👋 Bot Name: {bot_info['result']['first_name']}")
                else:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")
        except Exception as e:
            logger.error(f"🚫 Failed to connect to Telegram API: {e}")
            raise

        logger.info("🎉 FTM Boss Assistant initialized successfully and ready to monitor!")

    async def send_notification_to_telegram(self, notification: Dict):
        """Send notification to Telegram group as single message with PDF"""
        try:
            # Create notification text
            notification_text = f"📢 *New Circular Released*\n"

            # Add original title if configured
            if self.config.show_original_text:
                notification_text += f"*Original Title:* {notification['title']}\n"

            # Add translated title if translation is enabled
            if self.config.enable_translation:
                title_english = await self.translator.translate_to_english(notification['title'])
                if title_english and title_english != notification['title'] and not title_english.startswith('['):
                    notification_text += f"*English Title:* {title_english}\n"
                elif not self.config.show_original_text:
                    # If translation failed and we're not showing original, show original as fallback
                    notification_text += f"*Title:* {notification['title']}\n"
            elif not self.config.show_original_text:
                # If translation disabled and not showing original, show title without label
                notification_text += f"*Title:* {notification['title']}\n"

            notification_text += f"*Date:* {notification['date']}\n"

            # Add PDF link if available
            if notification.get('pdf_url'):
                notification_text += f"[View PDF]({notification['pdf_url']})"

            # Send as text message with link only
            message_data = {
                'chat_id': self.config.telegram_chat_id,
                'text': notification_text,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }
            async with self.session.post(f"{self.telegram_api_url}/sendMessage", data=message_data) as response:
                if response.status != 200:
                    raise Exception(f"Failed to send message: {await response.text()}")
                response_data = await response.json()
                message_id = response_data.get('result', {}).get('message_id')

            logger.info(f"Successfully sent notification {notification['id']} to Telegram (Message ID: {message_id})")
            return message_id

        except Exception as e:
            logger.error(f"Failed to send notification {notification['id']} to Telegram: {e}")
            raise

    async def process_telegram_commands(self):
        """Process Telegram commands like /send"""
        try:
            # Get updates from Telegram with proper offset tracking
            offset = getattr(self, '_last_update_id', 0) + 1
            async with self.session.get(f"{self.telegram_api_url}/getUpdates?offset={offset}&timeout=1") as response:
                if response.status != 200:
                    return

                data = await response.json()
                if not data.get('ok') or not data.get('result'):
                    return

                # Process each update
                for update in data['result']:
                    # Track the last update_id to avoid processing same message twice
                    update_id = update.get('update_id', 0)
                    if update_id > 0:
                        self._last_update_id = update_id
                    
                    message = update.get('message', {})
                    text = message.get('text', '').strip()
                    chat = message.get('chat', {})
                    chat_id = str(chat.get('id', ''))
                    chat_type = chat.get('type', '')

                    # Only process /new command
                    if not text.startswith('/new'):
                        continue

                    logger.info(f"📩 Received /new command from chat {chat_id} (type: {chat_type})")

                    # Check if it's from a group (not private message)
                    if chat_type not in ['group', 'supergroup']:
                        logger.warning(f"⚠️ /new command rejected: only works in groups, not {chat_type}")
                        error_msg = {
                            'chat_id': chat_id,
                            'text': "⚠️ This command only works in groups, not in private messages.",
                            'reply_to_message_id': message.get('message_id')
                        }
                        await self.session.post(f"{self.telegram_api_url}/sendMessage", data=error_msg)
                        continue

                    # Send acknowledgment
                    ack_msg = {
                        'chat_id': chat_id,
                        'text': "🔍 Fetching last 10 circulars from BKNMU website...",
                        'reply_to_message_id': message.get('message_id')
                    }
                    async with self.session.post(f"{self.telegram_api_url}/sendMessage", data=ack_msg) as ack_response:
                        if ack_response.status == 200:
                            logger.info("✅ Sent acknowledgment message")

                    # Fetch last 10 circulars
                    logger.info("🔍 Fetching last 10 circulars on demand...")
                    circulars = await self.scraper.scrape_notifications(limit=10)

                    if not circulars:
                        no_data_msg = {
                            'chat_id': chat_id,
                            'text': "❌ No circulars found on the website.",
                            'reply_to_message_id': message.get('message_id')
                        }
                        await self.session.post(f"{self.telegram_api_url}/sendMessage", data=no_data_msg)
                        continue

                    # Send each circular
                    sent_count = 0
                    for circular in circulars:
                        try:
                            # Temporarily override chat_id to send to the requesting group
                            original_chat_id = self.config.telegram_chat_id
                            self.config.telegram_chat_id = chat_id

                            await self.send_notification_to_telegram(circular)
                            sent_count += 1

                            # Restore original chat_id
                            self.config.telegram_chat_id = original_chat_id

                            # Small delay between messages
                            await asyncio.sleep(2)

                        except Exception as e:
                            logger.error(f"❌ Failed to send circular: {e}")
                            continue

                    # Send completion message
                    complete_msg = {
                        'chat_id': chat_id,
                        'text': f"✅ Successfully sent {sent_count} circulars!",
                        'reply_to_message_id': message.get('message_id')
                    }
                    await self.session.post(f"{self.telegram_api_url}/sendMessage", data=complete_msg)
                    logger.info(f"✅ Completed /new command - sent {sent_count} circulars")

        except Exception as e:
            logger.error(f"❌ Error processing Telegram commands: {e}")

    async def process_new_notifications(self):
        """Process and send new notifications"""
        try:
            logger.info("🔍 Scanning BKNMU website for new notifications...")

            # Scrape latest notifications
            notifications = await self.scraper.scrape_notifications()

            if not notifications:
                logger.info("📭 No notifications found on website")
                return

            logger.info(f"📄 Scraped {len(notifications)} total notifications from website")

            # Filter notifications from configured date onwards (inclusive)
            cutoff_date = datetime.strptime(self.config.filter_from_date, "%Y-%m-%d")
            new_notifications = []

            logger.info(f"📅 Filtering notifications from {self.config.filter_from_date} onwards...")

            for notification in notifications:
                # Check date filter first (more efficient)
                try:
                    notification_date = datetime.strptime(notification['date'], '%d/%m/%Y')
                    if notification_date < cutoff_date:
                        continue
                except ValueError:
                    logger.warning(f"⚠️  Invalid date format for notification {notification['id']}: {notification['date']}")
                    continue

                # Check if already processed by ID
                if await self.database.is_notification_processed(notification['id']):
                    logger.debug(f"⏭️  Notification {notification['id']} already processed by ID, skipping")
                    continue

                # Double-check by content to prevent duplicates with different IDs
                if await self.database.is_notification_processed_by_content(
                    notification['title'], 
                    notification.get('pdf_url'), 
                    notification.get('url')
                ):
                    logger.debug(f"⏭️  Notification already processed by content: {notification['title'][:50]}...")
                    continue

                new_notifications.append(notification)

            if not new_notifications:
                logger.info("✨ All notifications are already processed - no new content to send")
                return

            logger.info(f"🆕 Found {len(new_notifications)} new notifications to process")

            # Process each new notification one by one
            for i, notification in enumerate(new_notifications, 1):
                try:
                    logger.info(f"📤 Processing notification {i}/{len(new_notifications)}: {notification['title'][:50]}...")

                    # Triple-check if already processed to prevent any race conditions
                    if await self.database.is_notification_processed(notification['id']):
                        logger.debug(f"🔄 Notification {notification['id']} already processed during processing, skipping")
                        continue

                    # Mark as processed FIRST with a temporary flag
                    await self.database.mark_notification_processing(notification)
                    logger.debug(f"🔒 Marked notification {notification['id']} as processing")

                    # Send to Telegram
                    telegram_message_id = await self.send_notification_to_telegram(notification)

                    # Mark as successfully sent with message ID
                    await self.database.mark_notification_processed(
                        notification, 
                        telegram_message_id, 
                        self.config.telegram_chat_id
                    )

                    logger.info(f"✅ Successfully processed and sent notification {notification['id']}")

                    # Delay between notifications to avoid rate limiting
                    logger.debug("⏳ Waiting 3 seconds to avoid rate limiting...")
                    await asyncio.sleep(3)

                except Exception as e:
                    logger.error(f"❌ Failed to process notification {notification['id']}: {e}")
                    # Mark as failed to prevent retry on next run
                    try:
                        await self.database.mark_notification_failed(notification, str(e))
                        logger.warning(f"🔄 Marked notification {notification['id']} as failed in database")
                    except Exception as db_error:
                        logger.error(f"💾 Failed to mark notification as failed in database: {db_error}")
                    continue

            logger.info(f"🏁 Completed processing batch of {len(new_notifications)} notifications")

        except Exception as e:
            logger.error(f"💥 Critical error in process_new_notifications: {e}")
            # Add exponential backoff for SSL errors
            if "SSL" in str(e):
                logger.warning("🔐 SSL error detected, implementing extended retry delay...")
                await asyncio.sleep(60)  # Wait 1 minute for SSL issues

    async def command_monitor_loop(self):
        """Continuously monitor for Telegram commands"""
        logger.info("🎮 Starting continuous command monitoring...")
        
        while self.running:
            try:
                await self.process_telegram_commands()
                # Check every 2 seconds for instant response
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error in command monitor: {e}")
                await asyncio.sleep(5)

    async def run(self):
        """Main bot loop"""
        logger.info("🚀 Starting FTM Boss Assistant main monitoring loop...")
        self.running = True

        # Send startup message
        try:
            logger.info("📱 Sending startup notification to Telegram...")
            startup_data = {
                'chat_id': self.config.telegram_chat_id,
                'text': "🤖 *FTM Boss Assistant v2.0 Started*\n\n✅ Bot is now online and monitoring\n📊 Check interval: Every 2 minutes\n🎯 Target: BKNMU Circulars\n🔥 Status: Active\n\n💬 Commands:\n/new - Manually fetch last 10 circulars (groups only)\n⚡ Commands respond instantly!",
                'parse_mode': 'Markdown'
            }
            async with self.session.post(f"{self.telegram_api_url}/sendMessage", data=startup_data) as response:
                if response.status == 200:
                    logger.info("✅ Startup notification sent successfully")
                else:
                    logger.warning(f"⚠️  Failed to send startup message: {await response.text()}")
        except Exception as e:
            logger.error(f"❌ Failed to send startup message: {e}")

        error_count = 0
        cycle_count = 0

        logger.info("🔄 Entering main monitoring loop...")
        
        # Start command monitoring in background
        command_task = asyncio.create_task(self.command_monitor_loop())

        while self.running:
            try:
                cycle_count += 1
                logger.info(f"🔄 Starting monitoring cycle #{cycle_count}")

                await self.process_new_notifications()
                error_count = 0  # Reset error count on successful run

                logger.info("😴 Waiting 2 minutes before next scan...")

                # Wait for 2 minutes before next check
                for i in range(120):  # 120 seconds = 2 minutes
                    if not self.running:
                        logger.info("🛑 Shutdown signal received, stopping monitoring loop")
                        break

                    # Show progress every minute (60 seconds)
                    if i > 0 and i % 60 == 0:
                        remaining = 120 - i
                        remaining_minutes = remaining // 60
                        logger.debug(f"⏰ {remaining_minutes} minute(s) remaining until next scan...")

                    await asyncio.sleep(1)

            except Exception as e:
                error_count += 1
                logger.error(f"💥 Error in main loop (attempt #{error_count}): {e}")

                # Exponential backoff for repeated errors
                wait_time = min(300, 30 * (2 ** min(error_count - 1, 3)))  # Max 5 minutes
                logger.warning(f"🔄 Implementing exponential backoff: waiting {wait_time} seconds before retry...")

                if error_count >= 3:
                    logger.critical(f"🚨 Multiple consecutive errors detected! This may indicate a serious issue.")

                for i in range(wait_time):
                    if not self.running:
                        break
                    if i > 0 and i % 60 == 0:  # Show progress every minute for long waits
                        remaining = wait_time - i
                        logger.debug(f"⏳ {remaining} seconds remaining in error backoff...")
                    await asyncio.sleep(1)

        # Cancel command monitoring task
        command_task.cancel()
        try:
            await command_task
        except asyncio.CancelledError:
            logger.info("🎮 Command monitoring task stopped")

        logger.info("🛑 FTM Boss Assistant monitoring loop stopped")

    async def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Initiating cleanup process...")

        try:
            # Send shutdown message
            shutdown_data = {
                'chat_id': self.config.telegram_chat_id,
                'text': "🤖 *FTM Boss Assistant Shutting Down*\n\n⏹️ Bot is going offline\n📊 All processes stopped\n💾 Data saved successfully",
                'parse_mode': 'Markdown'
            }
            async with self.session.post(f"{self.telegram_api_url}/sendMessage", data=shutdown_data) as response:
                if response.status == 200:
                    logger.info("📱 Shutdown notification sent to Telegram")
        except Exception as e:
            logger.warning(f"⚠️  Failed to send shutdown notification: {e}")

        logger.info("🗄️  Closing database connections...")
        await self.database.close()

        logger.info("🌐 Closing HTTP session...")
        await self.session.close()

        logger.info("✅ Cleanup completed successfully")

        # Final goodbye banner
        print(f"""
{ColoredFormatter.COLORS['BOLD']}{ColoredFormatter.COLORS['INFO']}
╔══════════════════════════════════════════════════════════════════════════════╗
║                     👋 FTM Boss Assistant Shutdown Complete                 ║
║                        Thank you for using our service!                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
{ColoredFormatter.COLORS['RESET']}""")


def start_web_server():
    """Start the web server in a separate thread"""
    try:
        logger.info("🌐 Starting web server on port 5000...")
        from web_app import app, initialize_web_app
        
        # Initialize web app database
        initialize_web_app()
        
        # Run Flask app
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"❌ Failed to start web server: {e}")


async def main():
    """Main entry point"""
    logger.info("🎬 Starting FTM Boss Assistant application...")

    # Start web server in background thread
    web_thread = threading.Thread(target=start_web_server, daemon=True)
    web_thread.start()
    logger.info("✅ Web server thread started")

    bot = FTMBossAssistant()

    try:
        await bot.initialize()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("⌨️  Received keyboard interrupt - initiating graceful shutdown...")
    except Exception as e:
        logger.critical(f"💀 Fatal error occurred: {e}")
        logger.error("🚨 Application will now exit due to critical error")
        sys.exit(1)
    finally:
        await bot.cleanup()


if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())
