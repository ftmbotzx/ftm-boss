"""
Web scraper module for BKNMU notifications
Handles scraping of notifications from BKNMU website
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings for sites with certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import Config

logger = logging.getLogger(__name__)


class BKNMUScraper:
    """Web scraper for BKNMU notifications"""
    
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
        
        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Set default timeout for requests
        self.default_timeout = self.config.request_timeout
    
    def _make_request(self, url: str, max_retries: Optional[int] = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        max_retries = max_retries if max_retries is not None else self.config.max_retries
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=self.default_timeout, verify=False)
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed for {url}: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} attempts failed for {url}")
                    return None
                
                # Exponential backoff
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time} seconds before retry...")
                import time
                time.sleep(wait_time)
        
        return None
    
    def _extract_notification_id(self, title: str, date: str, pdf_url: str = None) -> str:
        """Generate unique ID for notification based on content"""
        try:
            # Create a hash-like ID from title, date, and PDF URL
            id_source = f"{date}_{title}_{pdf_url}".lower()
            id_hash = str(hash(id_source))
            return id_hash
            
        except Exception as e:
            logger.warning(f"Failed to extract notification ID: {e}")
            return str(hash(f"{title}_{date}"))
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to standard format DD/MM/YYYY"""
        try:
            # Clean the date string
            date_str = date_str.strip()
            
            # Handle different date formats
            date_patterns = [
                r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
                r'(\d{1,2})-(\d{1,2})-(\d{4})',  # DD-MM-YYYY
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})', # DD.MM.YYYY
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_str)
                if match:
                    day, month, year = match.groups()
                    return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date '{date_str}': {e}")
            return None
    
    def _parse_circular_row(self, row) -> Optional[Dict]:
        """Parse individual circular row from HTML table"""
        try:
            notification = {}
            
            # Find the link element
            link = row.find('a', href=True)
            if not link:
                return None
            
            # Extract PDF URL
            pdf_url = link.get('href', '')
            if pdf_url:
                # Make absolute URL
                if not pdf_url.startswith('http'):
                    pdf_url = urljoin(self.config.bknmu_base_url, pdf_url)
                notification['pdf_url'] = pdf_url
            else:
                notification['pdf_url'] = None
            
            # Extract title - get text before <br> tag
            title_parts = []
            for content in link.contents:
                if content.name == 'br':
                    break
                if isinstance(content, str):
                    title_parts.append(content.strip())
            
            title = ' '.join(title_parts).strip()
            if not title or len(title) < 5:
                return None
            
            notification['title'] = title
            
            # Extract date from <small> tag
            date_elem = link.find('small')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                parsed_date = self._parse_date(date_text)
                if parsed_date:
                    notification['date'] = parsed_date
                else:
                    return None
            else:
                return None
            
            # Set type
            notification['type'] = 'Circular'
            
            # Extract notification URL (same as PDF URL in this case)
            notification['url'] = pdf_url
            
            # Generate unique ID
            notification['id'] = self._extract_notification_id(
                notification['title'],
                notification['date'],
                notification.get('pdf_url')
            )
            
            return notification
            
        except Exception as e:
            logger.error(f"Error parsing circular row: {e}")
            return None
    
    async def scrape_notifications(self, limit: Optional[int] = None) -> List[Dict]:
        """Scrape notifications from BKNMU website
        
        Args:
            limit: Maximum number of circulars to return (None for all)
        """
        logger.info(f"Starting to scrape BKNMU circulars{f' (limit: {limit})' if limit else ''}...")
        
        def _scrape():
            """Synchronous scraping function to run in executor"""
            notifications = []
            
            # Make request to circulars page
            response = self._make_request(self.config.bknmu_circulars_url)
            if not response:
                logger.error("Failed to fetch BKNMU circulars page")
                return notifications
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all table rows with circular data
            # The circulars are in <tr> tags within a table
            rows = soup.find_all('tr')
            
            logger.info(f"Found {len(rows)} table rows to process")
            
            # Parse each row
            for row in rows:
                try:
                    circular = self._parse_circular_row(row)
                    if circular:
                        notifications.append(circular)
                        logger.debug(f"Parsed circular: {circular['date']} - {circular['title'][:50]}...")
                        
                        # Check if we've reached the limit
                        if limit and len(notifications) >= limit:
                            logger.info(f"Reached limit of {limit} circulars")
                            break
                            
                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue
            
            logger.info(f"Successfully scraped {len(notifications)} circulars")
            return notifications
        
        # Run scraping in executor to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _scrape)
    
    async def test_scraping(self):
        """Test the scraping functionality"""
        logger.info("Testing BKNMU scraping...")
        
        notifications = await self.scrape_notifications()
        
        if notifications:
            logger.info(f"Test successful! Found {len(notifications)} notifications")
            for i, notification in enumerate(notifications[:3]):  # Show first 3
                logger.info(f"  {i+1}. {notification['date']} - {notification['title'][:100]}...")
        else:
            logger.warning("Test failed - no notifications found")
        
        return notifications


# Test function for development
async def test_scraper():
    """Test function for the scraper"""
    scraper = BKNMUScraper()
    await scraper.test_scraping()


if __name__ == "__main__":
    # Run test
    asyncio.run(test_scraper())
