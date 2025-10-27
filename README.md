# FTM Boss Assistant - BKNMU Notification Scraper Bot

## Overview

FTM Boss Assistant is a Telegram bot that monitors the BKNMU (Bhakta Kavi Narsinh Mehta University) website for new notifications and automatically sends them to a configured Telegram chat. The bot scrapes the university's news and events page, translates Gujarati content to English using Google Translate, and stores processed notifications in a PostgreSQL database to avoid duplicate messages.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Bot Architecture
The application follows a modular architecture with clear separation of concerns:

- **Main Bot Controller** (`bot.py`): Orchestrates the entire workflow with signal handling for graceful shutdown
- **Configuration Management** (`config.py`): Centralized environment variable handling and validation
- **Database Layer** (`database.py`): PostgreSQL operations with connection pooling for notification tracking
- **Web Scraper** (`scraper.py`): BKNMU website scraping with retry logic and browser mimicking
- **Translation Service** (`translator.py`): Gujarati to English translation with caching

### Data Flow
1. Bot periodically scrapes BKNMU news/events page
2. New notifications are extracted and parsed
3. Gujarati text is translated to English using Google Translate
4. Notifications are checked against database to prevent duplicates
5. New notifications are sent to configured Telegram chat
6. Processed notifications are stored in database

### Database Design
Uses PostgreSQL with connection pooling for:
- Storing processed notification IDs to prevent duplicates
- Tracking notification metadata and processing timestamps
- Maintaining translation cache for efficiency

### Error Handling & Reliability
- Comprehensive logging to both file and console
- Retry logic for web requests and API calls
- Graceful shutdown handling with signal handlers
- Connection pooling for database reliability
- Request timeout and rate limiting considerations

### Security & Configuration
- Environment variable based configuration
- Required parameter validation on startup
- User-Agent spoofing to mimic real browser requests
- Secure session management for web scraping

## External Dependencies

### Core Services
- **Telegram Bot API**: For sending notifications to Telegram chats
- **PostgreSQL Database**: Persistent storage for notification tracking and deduplication
- **BKNMU Website** (`https://www.bknmu.edu.in/news-events`): Source of university notifications

### Translation Services
- **Google Translate API**: Primary translation service (requires API key)
- **googletrans Library**: Fallback free translation service

### Python Libraries
- **requests + BeautifulSoup**: Web scraping and HTML parsing
- **psycopg2**: PostgreSQL database connectivity with connection pooling
- **python-telegram-bot**: Telegram Bot API wrapper
- **googletrans**: Free Google Translate API client

### Infrastructure Requirements
- Python 3.7+ runtime environment
- PostgreSQL database instance
- Internet connectivity for web scraping and API calls
- Environment variables for configuration (Telegram bot token, chat ID, database credentials)