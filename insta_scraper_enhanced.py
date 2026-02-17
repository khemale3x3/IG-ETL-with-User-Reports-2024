import os
import json
import time
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from datetime import datetime
import itertools
import threading
import sys
import random
import psutil
import queue
from tqdm import tqdm
import concurrent.futures

SESSION_ID = ""
TARGET_QUERIES = {
    "profile": "user",
    "timeline": "xdt_api__v1__feed__user_timeline_graphql_connection"
}

TEST_MODE = False
MAX_TEST_PROFILES = 5
MAX_WORKERS = 3
FORCE_MAX_WORKERS = True
INPUT_FILE = "input.csv"
DONE_FILE = "inputdone.csv"

spinner_active = False
spinner_username = ""
stats_lock = threading.Lock()
done_urls_lock = threading.Lock()

def get_optimal_worker_count():
    """Determine optimal number of concurrent workers based on system resources."""
    if FORCE_MAX_WORKERS:
        log_message(f"Forcing {MAX_WORKERS} browser instances as requested", level="INFO", icon="")
        return MAX_WORKERS

    cpu_count = psutil.cpu_count(logical=False) or 2
    available_memory_gb = psutil.virtual_memory().available / (1024**3)

    memory_based_limit = max(1, int(available_memory_gb / 1.2))

    cpu_based_limit = max(1, cpu_count - 1)

    browser_count = min(memory_based_limit, cpu_based_limit)

    final_count = min(MAX_WORKERS, browser_count)
    log_message(f"System resources suggest {browser_count} browsers, using {final_count}", level="INFO", icon="")
    return final_count

def configure_driver(session_id=None, proxy=None):
    """Configure and create a new Chrome driver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-browser-side-navigation")
    options.add_argument("--disable-infobars")
    options.add_argument("--mute-audio")
    options.add_argument("--no-sandbox")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.managed_default_content_settings.videos": 2}
    options.add_experimental_option("prefs", prefs)
    log_message("Driver configured to not load images or videos", level="INFO", icon="✓")

    options.set_capability('goog:loggingPrefs', {'performance': 'ALL', 'browser': 'ALL'})

    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    try:
        driver = webdriver.Chrome(options=options)

        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})

        if session_id:
            driver.get("https://www.instagram.com/")
            driver.add_cookie({
                "name": "sessionid",
                "value": session_id,
                "domain": ".instagram.com",
                "path": "/",
                "secure": True,
                "httpOnly": True
            })
            log_message("Session ID set successfully", level="INFO", icon="")
        return driver
    except Exception as e:
        log_message(f"Failed to create Chrome driver: {str(e)}", level="ERROR")
        return None

def spinner():
    """Show a progress spinner in the console."""
    for char in itertools.cycle(['|', '/', '-', '\\']):
        if not spinner_active:
            break
        sys.stdout.write(f'\r\033[33mScraping: {spinner_username} {char}\033[0m')
        sys.stdout.flush()
        time.sleep(0.1)

def log_message(message, level="INFO", icon=""):
    """Log a colored message to the console with a timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = "\033[0m"
    if level == "INFO":
        color = "\033[94m"
    elif level == "SUCCESS":
        color = "\033[92m"
    elif level == "WARNING":
        color = "\033[93m"
    elif level == "ERROR":
        color = "\033[91m"

    ascii_icon = "*"
    if level == "INFO":
        ascii_icon = "i"
    elif level == "SUCCESS":
        ascii_icon = "√"
    elif level == "WARNING":
        ascii_icon = "!"
    elif level == "ERROR":
        ascii_icon = "×"

    try:
        print(f"{color}[{timestamp}] [{ascii_icon}] {message}\033[0m")
    except UnicodeEncodeError:
        print(f"[{timestamp}] [{level}] {message}")

def get_username(url):
    """Extract the username from a given Instagram URL."""
    url = url.strip().rstrip('/')
    username = url.split('/')[-1]
    username = username.split('?')[0]
    return username

def is_private_profile(profile_data):
    """Check if a profile is private based on its data."""
    if not profile_data:
        return True
    try:
        return profile_data["data"]["user"]["is_private"]
    except (KeyError, TypeError):
        return True

def process_graphql_response(response_body):
    """Process a GraphQL response to extract profile and timeline data."""
    data = {"profile_info": None, "reel_info": None}

    if not isinstance(response_body, dict):
        return data

    response_data = response_body.get("data", {})

    if TARGET_QUERIES["profile"] in response_data:
        data["profile_info"] = response_body

    if TARGET_QUERIES["timeline"] in response_data:
        data["reel_info"] = response_body

    return data

def calculate_dynamic_wait_time(profile_data):
    """Calculate a dynamic wait time based on profile complexity and randomization."""
    base_wait = random.uniform(1.0, 2.0)

    if profile_data and "data" in profile_data and "user" in profile_data["data"]:
        user_data = profile_data["data"]["user"]

        follower_count = user_data.get("edge_followed_by", {}).get("count", 0)

        post_count = user_data.get("edge_owner_to_timeline_media", {}).get("count", 0)

        complexity = (follower_count / 500000) + (post_count / 5000)

        additional_wait = min(complexity, 1.0)

        return base_wait + additional_wait

    return base_wait

def get_network_responses(driver):
    """Extract network responses from browser logs."""
    logs = driver.get_log("performance")
    responses = []

    for log in logs:
        try:
            log_data = json.loads(log["message"])["message"]
            if "Network.response" in log_data["method"] or "Network.responseReceived" in log_data["method"]:
                responses.append(log_data)
        except Exception:
            pass

    return responses

def scrape_profile(driver, url):
    """Scrape a single Instagram profile using Selenium with enhanced post collection."""
    global spinner_active, spinner_username
    spinner_username = get_username(url)
    spinner_active = True
    spinner_thread = threading.Thread(target=spinner)
    spinner_thread.start()

    try:
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
        driver.get("about:blank")
        driver.execute_cdp_cmd("Network.enable", {})

        driver.get(url)
        time.sleep(random.uniform(2.0, 3.0))

        scroll_attempts = 0
        MAX_SCROLL_ATTEMPTS = 15  # Increased from 8 to 15 for more posts
        posts_before = 0
        no_new_posts_count = 0
        combined_data = {"profile_info": None, "reel_info": None}
        target_posts = 50  # Target 50+ posts

        while scroll_attempts < MAX_SCROLL_ATTEMPTS:
            try:
                responses = get_network_responses(driver)

                for response in responses:
                    try:
                        if "params" in response and "response" in response["params"]:
                            response_url = response["params"]["response"].get("url", "")

                            if "graphql/query" in response_url:
                                request_id = response["params"].get("requestId")
                                if not request_id:
                                    continue

                                body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                                response_body = json.loads(body["body"])
                                new_data = process_graphql_response(response_body)

                                if new_data["profile_info"]:
                                    combined_data["profile_info"] = new_data["profile_info"]
                                if new_data["reel_info"]:
                                    combined_data["reel_info"] = merge_timeline_data(
                                        combined_data["reel_info"],
                                        new_data["reel_info"]
                                    )
                    except Exception as e:
                        continue

                posts_after = len(combined_data["reel_info"]["data"][TARGET_QUERIES["timeline"]]["edges"]) if combined_data["reel_info"] else 0

                if posts_after >= target_posts:
                    log_message(f"Reached target of {target_posts} posts, stopping at {posts_after} posts", level="INFO")
                    break

                if posts_after == posts_before:
                    no_new_posts_count += 1
                    if no_new_posts_count >= 3 and posts_after >= 30:  # Stop if we have at least 30 posts
                        log_message(f"No new posts after 3 scrolls, stopping at {posts_after} posts", level="INFO")
                        break
                else:
                    no_new_posts_count = 0
                    posts_before = posts_after
                    log_message(f"Found {posts_after} posts so far (target: {target_posts})", level="INFO")

                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(random.uniform(2.5, 4.0))  # Slightly longer wait for more posts
                scroll_attempts += 1

            except Exception as scroll_error:
                log_message(f"Scroll error: {str(scroll_error)}", level="WARNING")
                break

        if combined_data["reel_info"]:
            edges = combined_data["reel_info"]["data"][TARGET_QUERIES["timeline"]]["edges"]
            for edge in edges:
                node = edge.get("node", {})
                shortcode = node.get("shortcode", "")
                if shortcode:
                    node["post_url"] = f"https://www.instagram.com/p/{shortcode}/"
                    node["post_code"] = shortcode

        spinner_active = False
        spinner_thread.join()
        return combined_data

    except Exception as e:
        spinner_active = False
        if spinner_thread.is_alive():
            spinner_thread.join()
        log_message(f"Error scraping profile: {str(e)}", level="ERROR")
        return {"profile_info": None, "reel_info": None}

def merge_timeline_data(existing_data, new_data):
    """Merge new timeline data with existing data to avoid duplicates."""
    if not existing_data:
        return new_data
    if not new_data:
        return existing_data

    try:
        timeline_key = TARGET_QUERIES["timeline"]
        existing_edges = existing_data["data"][timeline_key]["edges"]
        new_edges = new_data["data"][timeline_key]["edges"]

        existing_ids = {edge["node"]["id"] for edge in existing_edges}

        for edge in new_edges:
            if edge["node"]["id"] not in existing_ids:
                existing_edges.append(edge)
                existing_ids.add(edge["node"]["id"])

        existing_data["data"][timeline_key]["edges"] = existing_edges
        return existing_data
    except Exception as e:
        log_message(f"Error merging timeline data: {str(e)}", level="ERROR")
        return existing_data

def save_data(username, data, url, no_response_links, stats, done_urls):
    """Save the scraped data to a JSON file and handle failures."""
    if is_private_profile(data["profile_info"]) or not (data["profile_info"] or data["reel_info"]):
        with stats_lock:
            no_response_links.append(url)
            stats["failed"] += 1
        log_message(f"Private profile/no data: {username}", level="WARNING", icon="")
        return False

    user_dir = f"output/{username}"
    os.makedirs(user_dir, exist_ok=True)

    success = False
    if data["profile_info"]:
        with open(f"{user_dir}/userInfo.json", "w") as f:
            json.dump(data["profile_info"], f, indent=4)
        with stats_lock:
            stats["saved"] += 1
        log_message(f"Saved public profile: {username}", level="INFO", icon="")
        success = True

        if download_profile_picture(username, data["profile_info"], user_dir):
            with stats_lock:
                stats["pictures_downloaded"] += 1

    if data["reel_info"]:
        with open(f"{user_dir}/postInfo.json", "w") as f:
            json.dump(data["reel_info"], f, indent=4)

    wait_time = calculate_dynamic_wait_time(data["profile_info"])
    time.sleep(wait_time)

    if success:
        with done_urls_lock:
            done_urls.append(url)
            save_url_to_done_file(url)

    return success

def save_url_to_done_file(url):
    """Save a single URL to the done file and remove it from the input file."""
    try:
        if not os.path.exists(DONE_FILE):
            with open(DONE_FILE, 'w') as f:
                f.write("url\n")

        with open(DONE_FILE, 'a') as f:
            f.write(f"{url}\n")

        remove_url_from_input_file(url)

        log_message(f"URL moved from {INPUT_FILE} to {DONE_FILE}: {url}", level="INFO", icon="")
        return True
    except Exception as e:
        log_message(f"Error managing URL files: {str(e)}", level="ERROR", icon="")
        return False

def remove_url_from_input_file(url):
    """Remove a URL from the input file."""
    try:
        input_df = pd.read_csv(INPUT_FILE)

        input_df = input_df[input_df['url'] != url]

        input_df.to_csv(INPUT_FILE, index=False)

        return True
    except Exception as e:
        log_message(f"Error removing URL from input file: {str(e)}", level="ERROR", icon="")
        return False

def download_profile_picture(username, profile_info, save_dir):
    """Download a profile picture from its URL with retries."""
    try:
        pic_url = None
        try:
            pic_url = profile_info["data"]["user"].get("profile_pic_url_hd") or profile_info["data"]["user"]["profile_pic_url"]
        except (KeyError, TypeError):
            log_message(f"Could not find profile picture URL in data structure", level="WARNING", icon="")
            return False

        if not pic_url:
            log_message(f"No profile picture URL found for {username}", level="WARNING", icon="")
            return False

        for attempt in range(3):
            try:
                response = requests.get(pic_url, stream=True, timeout=10)
                if response.status_code == 200:
                    ext = "jpg" if ".jpg" in pic_url.lower() else "png"
                    file_path = os.path.join(save_dir, f"{username}.{ext}")

                    with open(file_path, "wb") as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)

                    log_message(f"Downloaded profile picture for {username}", level="INFO", icon="")
                    return True
                else:
                    log_message(f"Attempt {attempt+1}: Failed to download (HTTP {response.status_code})", level="WARNING", icon="")
            except Exception as e:
                log_message(f"Attempt {attempt+1}: Download error - {str(e)}", level="WARNING", icon="")
            time.sleep(1)

        log_message(f"Failed to download profile picture after 3 attempts", level="ERROR", icon="")
        return False
    except Exception as e:
        log_message(f"Profile picture download failed: {str(e)}", level="ERROR")
        return False

def worker_thread(url_queue, session_id, stats, no_response_links, progress_bar, done_urls):
    """Worker thread that processes URLs from a queue."""
    driver = None
    try:
        driver = configure_driver(session_id=session_id)
        if not driver:
            log_message("Failed to create browser instance for worker", level="ERROR")
            return

        while not url_queue.empty():
            try:
                url = url_queue.get(block=False)
            except queue.Empty:
                break

            try:
                username = get_username(url)
                log_message(f"Processing: {username}", level="INFO", icon="")

                data = scrape_profile(driver, url)
                save_data(username, data, url, no_response_links, stats, done_urls)

                progress_bar.update(1)

            except Exception as e:
                log_message(f"Failed to process {url}: {str(e)}", level="ERROR")
                with stats_lock:
                    no_response_links.append(url)
                    stats["failed"] += 1
                progress_bar.update(1)

            finally:
                url_queue.task_done()
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def load_urls():
    """Load URLs from input file, skipping those already in done file."""
    input_urls = []
    done_urls = []

    try:
        df = pd.read_csv(INPUT_FILE)
        input_urls = df["url"].tolist()
        log_message(f"Loaded {len(input_urls)} URLs from {INPUT_FILE}", level="INFO", icon="")
    except Exception as e:
        log_message(f"Error reading input file: {str(e)}", level="ERROR")
        return [], []

    if os.path.exists(DONE_FILE):
        try:
            df_done = pd.read_csv(DONE_FILE)
            done_urls = df_done["url"].tolist()

            done_urls = [url.strip().rstrip('/') for url in done_urls]
            log_message(f"Loaded {len(done_urls)} already completed URLs from {DONE_FILE}", level="INFO", icon="")
        except Exception as e:
            log_message(f"Error reading done file: {str(e)}", level="WARNING")

    normalized_input = [url.strip().rstrip('/') for url in input_urls]
    normalized_done = set(done_urls)

    urls_to_process = []
    for i, url in enumerate(input_urls):
        norm_url = normalized_input[i]
        if norm_url not in normalized_done:
            urls_to_process.append(url)

    skipped_count = len(input_urls) - len(urls_to_process)
    log_message(f"{skipped_count} URLs already processed, {len(urls_to_process)} URLs remaining", level="INFO", icon="")

    return urls_to_process, done_urls

def detect_account_type(profile_data):
    """Detect if account is personal, business, or creator based on profile data."""
    if not profile_data or "data" not in profile_data or "user" not in profile_data["data"]:
        return "unknown"
    
    user = profile_data["data"]["user"]
    
    # Check for business account indicators
    is_business_account = user.get("is_business_account", False)
    business_category = user.get("business_category_name", "")
    contact_phone = user.get("business_phone_number", "")
    contact_email = user.get("business_email", "")
    
    # Check for creator/professional account
    is_professional_account = user.get("is_professional_account", False)
    
    # Check biography for business indicators
    bio = user.get("biography", "").lower()
    business_keywords = ["ceo", "founder", "business", "company", "brand", "shop", "store", "service", "contact", "email", "dm for", "inquiries"]
    creator_keywords = ["creator", "influencer", "content", "youtube", "tiktok", "collab", "pr", "brand partnerships"]
    
    bio_business_score = sum(1 for keyword in business_keywords if keyword in bio)
    bio_creator_score = sum(1 for keyword in creator_keywords if keyword in bio)
    
    # Determine account type
    if is_business_account or business_category or contact_phone or contact_email or bio_business_score >= 2:
        return "business"
    elif is_professional_account or bio_creator_score >= 2:
        return "creator"
    else:
        return "personal"

def main():
    """Main function to orchestrate the scraping process."""
    log_message("Starting Instagram Scraper (Parallel Processing)", level="INFO", icon="")
    worker_count = get_optimal_worker_count()
    log_message(f"Using {worker_count} parallel browsers for optimized scraping", level="INFO", icon="")

    if TEST_MODE:
        log_message(f"Running in TEST MODE - will only process {MAX_TEST_PROFILES} profiles", level="INFO", icon="")

    start_time = time.time()

    stats = {
        "total": 0,
        "saved": 0,
        "failed": 0,
        "pictures_downloaded": 0
    }

    urls_to_process, done_urls = load_urls()

    if TEST_MODE and len(urls_to_process) > MAX_TEST_PROFILES:
        urls_to_process = urls_to_process[:MAX_TEST_PROFILES]

    no_response_links = []
    stats["total"] = len(urls_to_process)

    if len(urls_to_process) == 0:
        log_message("No new URLs to process.", level="INFO", icon="")
        return

    os.makedirs("output", exist_ok=True)

    url_queue = queue.Queue()
    for url in urls_to_process:
        url_queue.put(url)

    threads = []
    newly_completed_urls = []

    with tqdm(total=len(urls_to_process), desc="Processing Profiles") as progress_bar:
        for _ in range(worker_count):
            thread = threading.Thread(
                target=worker_thread,
                args=(url_queue, SESSION_ID, stats, no_response_links, progress_bar, newly_completed_urls)
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)

        url_queue.join()

    if no_response_links:
        pd.DataFrame({"url": no_response_links}).to_csv("output/noResponse.csv", index=False)

    log_message("URLs were saved to done file in real-time during processing", level="INFO", icon="")

    log_message("\n" + "="*50, level="INFO", icon="")
    log_message(f"Total URLs processed: {stats['total']}", level="INFO", icon="")
    log_message(f"Successfully saved profiles: {stats['saved']}", level="INFO", icon="")
    log_message(f"Profile pictures downloaded: {stats['pictures_downloaded']}", level="INFO", icon="")
    log_message(f"Failed/Private profiles: {stats['failed']}", level="INFO", icon="")
    log_message(f"Completed in {time.time() - start_time:.2f} seconds", level="INFO", icon="")
    log_message("="*50, level="INFO", icon="")

if __name__ == "__main__":
    main()
