import os
import json
import datetime
import re
import statistics
from pathlib import Path
from typing import List, Dict
from collections import Counter
from colorama import init, Fore, Style
import copy

# Initialize colorama
init(autoreset=True)

class InstagramAnalyzer:
    def __init__(self, output_dir="output"):
        self.output_dir = output_dir
        # Load cities DB for location matching (project-level data/us_cities_database.json)
        self.cities_db = self.load_cities_database()
        
    def load_cities_database(self):
        """Load the US cities database for location matching. Returns list or empty list."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            candidate = os.path.join(project_root, 'data', 'us_cities_database.json')
            if not os.path.exists(candidate):
                candidate = os.path.join(script_dir, 'data', 'us_cities_database.json')
            with open(candidate, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load cities database: {e}")
            return []

    def log_message(self, message, level="INFO"):
        """Log a colored message to the console with a timestamp."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        
    def load_json_file(self, file_path: str) -> dict:
        """Load and parse a JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            print(f"{Fore.RED}Error loading JSON file {file_path}: {str(e)}{Style.RESET_ALL}")
            return {}

    def filter_posts_by_date(self, posts_data, days_ago):
        """Filters posts to include only those from the last `days_ago`."""
        if not posts_data or not posts_data.get("data"):
            return {"data": {"xdt_api__v1__feed__user_timeline_graphql_connection": {"edges": []}}}

        filtered_posts_data = copy.deepcopy(posts_data)
        edges = filtered_posts_data["data"].get("xdt_api__v1__feed__user_timeline_graphql_connection", {}).get("edges", [])
        
        cutoff_timestamp = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).timestamp()
        
        filtered_edges = []
        for edge in edges:
            node = edge.get("node", {})
            taken_at = node.get("taken_at")
            if taken_at and taken_at >= cutoff_timestamp:
                filtered_edges.append(edge)
        
        filtered_posts_data["data"]["xdt_api__v1__feed__user_timeline_graphql_connection"]["edges"] = filtered_edges
        return filtered_posts_data

    def analyze_location_data(self, posts_data):
        """Analyze location data from posts including tags, hashtags, and timezone."""
        location_analysis = {
            'location_tags': [],
            'location_hashtags': [],
            'most_visited_locations': [],
            'timezone_estimate': 'Unknown',
            'posting_timezone_pattern': {},
            'travel_frequency': 0,
            'location_diversity_score': 0,
            'matched_us_cities': []
        }
        
        try:
            edges = posts_data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {}).get("edges", [])
            
            locations = []
            location_hashtags = set()
            posting_times = []
            
            for edge in edges:
                node = edge.get("node", {})
                
                # Primary: Extract location tags (standard Instagram location object)
                location = node.get("location")
                if location:
                    # prefer explicit name fields
                    location_name = location.get("name") or location.get("title") or location.get("address") or ""
                    if location_name:
                        locations.append(location_name)
                
                # Additional: Extract location information from other possible JSON fields
                # (some scrapers include different keys)
                for fld in ("location_name", "city", "city_name", "address", "place_name"):
                    try:
                        v = node.get(fld)
                        if v and isinstance(v, str) and v.strip():
                            locations.append(v.strip())
                    except Exception:
                        pass
                
                # Some scrapers include address JSON blobs
                addr_json = node.get("business_address_json") or node.get("address_json")
                if isinstance(addr_json, dict):
                    # try common keys
                    for k in ("city", "town", "state", "country", "street"):
                        val = addr_json.get(k)
                        if val:
                            locations.append(val)
                
                # Extract location-based hashtags from caption and other text fields
                caption = node.get("caption", {}).get("text", "") if node.get("caption") else ""
                if caption:
                    hashtags = re.findall(r'#(\w+)', caption.lower())
                    
                    # Common location hashtag patterns
                    location_patterns = [
                        r'.*city$', r'.*town$', r'.*beach$', r'.*park$', r'.*street$',
                        r'.*travel$', r'.*trip$', r'.*vacation$', r'.*holiday$',
                        r'^nyc$', r'^la$', r'^sf$', r'^london$', r'^paris$', r'^tokyo$',
                        r'^miami$', r'^vegas$', r'^chicago$', r'^boston$'
                    ]
                    
                    for hashtag in hashtags:
                        for pattern in location_patterns:
                            if re.match(pattern, hashtag):
                                location_hashtags.add(hashtag)
                                break
                
                # Also check caption and other textual fields for city-like words
                text_fields = [caption, node.get("title", "") or "", node.get("description", "") or ""]
                combined_text = " ".join([t for t in text_fields if t])
                # simple heuristic: words capitalized or common city words
                city_candidates = re.findall(r'\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\b', combined_text)
                for cand in city_candidates:
                    locations.append(cand)
                
                # Extract posting times for timezone analysis
                timestamp = node.get("taken_at")
                if timestamp:
                    try:
                        dt = datetime.datetime.fromtimestamp(timestamp)
                        posting_times.append(dt.hour)
                    except Exception:
                        pass
            
            # Normalize and dedupe locations
            normalized_locations = []
            for loc in locations:
                if not loc or not isinstance(loc, str):
                    continue
                cleaned = loc.strip()
                if cleaned and cleaned not in normalized_locations:
                    normalized_locations.append(cleaned)
            
            # Analyze locations
            if normalized_locations:
                location_counts = Counter(normalized_locations)
                location_analysis['location_tags'] = list(location_counts.keys())
                location_analysis['most_visited_locations'] = [loc for loc, count in location_counts.most_common(10)]
                location_analysis['travel_frequency'] = len(set(normalized_locations))
                location_analysis['location_diversity_score'] = min(100, len(set(normalized_locations)) * 5)
            
            location_analysis['location_hashtags'] = list(location_hashtags)
            
            # Try to match found locations against US cities DB (if loaded)
            matched_cities = []
            if self.cities_db and normalized_locations:
                # cities_db assumed to be list of dicts with keys like 'city' and 'state' or 'name'
                for loc in normalized_locations:
                    lower_loc = loc.lower()
                    for entry in self.cities_db:
                        try:
                            entry_name = (entry.get('city') or entry.get('name') or "").lower()
                            entry_state = entry.get('state') or entry.get('region') or ""
                            if not entry_name:
                                continue
                            if lower_loc == entry_name or lower_loc.startswith(entry_name) or entry_name in lower_loc:
                                matched_cities.append({
                                    'input_location': loc,
                                    'matched_city': entry_name,
                                    'state': entry_state
                                })
                                break
                        except Exception:
                            continue
            location_analysis['matched_us_cities'] = matched_cities
            
            # Analyze posting timezone patterns
            if posting_times:
                hour_counts = Counter(posting_times)
                location_analysis['posting_timezone_pattern'] = dict(hour_counts)
                
                # Estimate timezone based on peak posting hours
                most_active_hours = [hour for hour, count in hour_counts.most_common(5)]
                avg_posting_hour = statistics.mean(most_active_hours)
                
                # Simple timezone estimation based on peak posting hours
                if 6 <= avg_posting_hour <= 10:
                    location_analysis['timezone_estimate'] = 'Morning poster (likely local timezone)'
                elif 11 <= avg_posting_hour <= 15:
                    location_analysis['timezone_estimate'] = 'Afternoon poster (likely local timezone)'
                elif 16 <= avg_posting_hour <= 22:
                    location_analysis['timezone_estimate'] = 'Evening poster (likely local timezone)'
                else:
                    location_analysis['timezone_estimate'] = 'Night poster (possible different timezone)'
        
        except Exception as e:
            self.log_message(f"Error analyzing location data: {str(e)}", level="WARNING")
        
        return location_analysis

    def detect_fake_followers(self, user_data, posts_data):
        """Detect potential fake followers based on engagement patterns and metrics."""
        fake_follower_analysis = {
            'fake_follower_score': 0,
            'suspicious_indicators': [],
            'authenticity_score': 100,
            'engagement_quality': 'Good',
            'comment_quality_score': 0,
            'follower_growth_pattern': 'Natural'
        }
        
        try:
            follower_count = user_data.get('follower_count', 0)
            following_count = user_data.get('following_count', 0)
            
            # Calculate engagement metrics
            edges = posts_data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {}).get("edges", [])
            
            if not edges or follower_count == 0:
                return fake_follower_analysis
            
            total_likes = 0
            total_comments = 0
            post_count = len(edges)
            
            for edge in edges[:20]:  # Analyze recent 20 posts
                node = edge.get("node", {})
                total_likes += node.get("like_count", 0)
                total_comments += node.get("comment_count", 0)
            
            if post_count > 0:
                avg_likes = total_likes / post_count
                avg_comments = total_comments / post_count
                engagement_rate = ((total_likes + total_comments) / post_count) / follower_count * 100
                
                # Suspicious indicators
                suspicious_indicators = []
                
                # Very low engagement rate
                if engagement_rate < 0.5:
                    suspicious_indicators.append("Very low engagement rate")
                    fake_follower_analysis['fake_follower_score'] += 30
                
                # High follower to following ratio but low engagement
                if follower_count > 10000 and following_count < 100 and engagement_rate < 1:
                    suspicious_indicators.append("High follower count with very low following and engagement")
                    fake_follower_analysis['fake_follower_score'] += 25
                
                # Very few comments relative to likes
                if avg_likes > 100 and avg_comments < avg_likes * 0.01:
                    suspicious_indicators.append("Disproportionately low comment rate")
                    fake_follower_analysis['fake_follower_score'] += 20
                
                fake_follower_analysis['suspicious_indicators'] = suspicious_indicators
                fake_follower_analysis['authenticity_score'] = max(0, 100 - fake_follower_analysis['fake_follower_score'])
                
                if fake_follower_analysis['fake_follower_score'] > 50:
                    fake_follower_analysis['engagement_quality'] = 'Poor'
                elif fake_follower_analysis['fake_follower_score'] > 25:
                    fake_follower_analysis['engagement_quality'] = 'Moderate'
                else:
                    fake_follower_analysis['engagement_quality'] = 'Good'
        
        except Exception as e:
            self.log_message(f"Error detecting fake followers: {str(e)}", level="WARNING")
        
        return fake_follower_analysis

    def analyze_profile_personality(self, bio, posts_data, engagement_metrics):
        """Analyze profile personality and create insights."""
        personality_traits = {
            'professional': 0,
            'creative': 0,
            'social': 0,
            'authentic': 0,
            'aspirational': 0,
            'educational': 0,
            'entertaining': 0
        }
        
        profile_insights = {
            'personality_type': 'Balanced',
            'content_style': 'Mixed',
            'audience_appeal': 'General',
            'brand_potential': 'Medium',
            'authenticity_score': 50
        }
        
        try:
            if bio:
                bio_lower = bio.lower()
                
                # Professional indicators
                professional_words = ['ceo', 'founder', 'entrepreneur', 'business', 'professional', 'expert', 'consultant', 'coach']
                personality_traits['professional'] = sum(1 for word in professional_words if word in bio_lower)
                
                # Creative indicators
                creative_words = ['artist', 'creative', 'designer', 'photographer', 'writer', 'musician', 'creator']
                personality_traits['creative'] = sum(1 for word in creative_words if word in bio_lower)
                
                # Social indicators
                social_words = ['love', 'friends', 'family', 'community', 'together', 'sharing', 'connect']
                personality_traits['social'] = sum(1 for word in social_words if word in bio_lower)
            
            # Analyze posting patterns
            post_frequency = engagement_metrics.get('post_frequency', '')
            consistency = engagement_metrics.get('consistency_score', 0)
            
            if 'daily' in post_frequency.lower():
                personality_traits['authentic'] += 2
            
            if consistency > 70:
                personality_traits['professional'] += 1
            
            # Determine dominant personality trait
            max_trait = max(personality_traits.items(), key=lambda x: x[1])
            if max_trait[1] > 0:
                profile_insights['personality_type'] = max_trait[0].title()
            
            # Calculate authenticity score
            authenticity_factors = [
                consistency / 100 * 30,  # Consistency contributes 30%
                min(personality_traits['social'] * 10, 30),  # Social engagement 30%
                min(engagement_metrics.get('engagement_rate', 0) * 2, 40)  # Engagement rate 40%
            ]
            profile_insights['authenticity_score'] = round(sum(authenticity_factors))
            
            # Determine brand potential
            brand_score = (
                personality_traits['professional'] * 20 +
                engagement_metrics.get('engagement_rate', 0) * 5 +
                consistency
            )
            
            if brand_score > 150:
                profile_insights['brand_potential'] = 'High'
            elif brand_score > 75:
                profile_insights['brand_potential'] = 'Medium'
            else:
                profile_insights['brand_potential'] = 'Low'
        
        except Exception as e:
            self.log_message(f"Error analyzing profile personality: {str(e)}", level="WARNING")
        
        return personality_traits, profile_insights

    def calculate_engagement_metrics(self, posts_data, follower_count):
        """Calculate detailed engagement metrics with enhanced analysis."""
        metrics = {
            'avg_likes': 0,
            'avg_comments': 0,
            'avg_shares': 0,
            'engagement_rate': 0,
            'post_frequency': 'Unknown',
            'engagement_trend': 'Stable',
            'best_posting_time': 'Unknown',
            'total_posts_analyzed': 0,
            'consistency_score': 0,
            'viral_posts_count': 0,
            'avg_engagement_per_post': 0,
            'latest_posts_er': {}
        }
        
        try:
            edges = posts_data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {}).get("edges", [])
            
            if not edges or follower_count == 0:
                return metrics
            
            likes = []
            comments = []
            shares = []
            timestamps = []
            posting_hours = []
            engagement_scores = []
            
            for edge in edges:
                node = edge.get("node", {})
                
                like_count = node.get("like_count", 0)
                comment_count = node.get("comment_count", 0)
                share_count = node.get("share_count", 0)
                timestamp = node.get("taken_at")
                
                likes.append(like_count)
                comments.append(comment_count)
                shares.append(share_count)
                
                # Calculate engagement score for this post
                engagement_score = (like_count + comment_count * 2) / max(follower_count, 1) * 100
                engagement_scores.append(engagement_score)
                
                if timestamp:
                    timestamps.append(timestamp)
                    dt = datetime.datetime.fromtimestamp(timestamp)
                    posting_hours.append(dt.hour)
            
            metrics['total_posts_analyzed'] = len(likes)
            
            # Calculate averages
            if likes:
                metrics['avg_likes'] = round(statistics.mean(likes))
                metrics['avg_comments'] = round(statistics.mean(comments))
                metrics['avg_shares'] = round(statistics.mean(shares))
                
                # Enhanced engagement rate calculation
                total_engagement = sum(likes) + sum(comments)
                metrics['engagement_rate'] = round((total_engagement / len(likes)) / follower_count * 100, 2)
                metrics['avg_engagement_per_post'] = round(total_engagement / len(likes))
                
                # Consistency score (lower standard deviation = more consistent)
                if len(engagement_scores) > 1:
                    std_dev = statistics.stdev(engagement_scores)
                    mean_engagement = statistics.mean(engagement_scores)
                    metrics['consistency_score'] = round(max(0, 100 - (std_dev / max(mean_engagement, 0.1) * 100)), 1)
                
                # Count viral posts (posts with engagement > 2x average)
                avg_engagement = statistics.mean(engagement_scores)
                metrics['viral_posts_count'] = sum(1 for score in engagement_scores if score > avg_engagement * 2)
            
            # Posting frequency analysis
            if len(timestamps) > 1:
                timestamps.sort(reverse=True)
                time_diffs = []
                for i in range(len(timestamps) - 1):
                    diff_days = (timestamps[i] - timestamps[i + 1]) / (24 * 3600)
                    time_diffs.append(diff_days)
                
                if time_diffs:
                    avg_days_between = statistics.mean(time_diffs)
                    if avg_days_between < 0.5:
                        metrics['post_frequency'] = 'Multiple times daily'
                    elif avg_days_between < 1:
                        metrics['post_frequency'] = 'Daily'
                    elif avg_days_between < 2:
                        metrics['post_frequency'] = 'Every other day'
                    elif avg_days_between < 7:
                        metrics['post_frequency'] = 'Weekly'
                    elif avg_days_between < 14:
                        metrics['post_frequency'] = 'Bi-weekly'
                    else:
                        metrics['post_frequency'] = 'Monthly or less'
            
            # Best posting time analysis
            if posting_hours:
                hour_counts = Counter(posting_hours)
                most_common_hour = hour_counts.most_common(1)[0][0]
                if 6 <= most_common_hour <= 11:
                    metrics['best_posting_time'] = 'Morning (6-11 AM)'
                elif 12 <= most_common_hour <= 17:
                    metrics['best_posting_time'] = 'Afternoon (12-5 PM)'
                elif 18 <= most_common_hour <= 22:
                    metrics['best_posting_time'] = 'Evening (6-10 PM)'
                else:
                    metrics['best_posting_time'] = 'Night (11 PM-5 AM)'
            
            # Engagement trend analysis
            if len(likes) >= 10:
                mid_point = len(likes) // 2
                recent_avg = statistics.mean(likes[:mid_point])
                older_avg = statistics.mean(likes[mid_point:])
                
                if recent_avg > older_avg * 1.15:
                    metrics['engagement_trend'] = 'Growing'
                elif recent_avg < older_avg * 0.85:
                    metrics['engagement_trend'] = 'Declining'
                else:
                    metrics['engagement_trend'] = 'Stable'

            # New: Post Engagement Stats for the last 6 posts
            latest_posts = sorted(edges, key=lambda x: x.get("node", {}).get("taken_at", 0), reverse=True)[:6]
            latest_posts_er_data = {}
            total_latest_er = 0
            for i, post_edge in enumerate(latest_posts):
                node = post_edge.get("node", {})
                likes_count = node.get("like_count", 0)
                comments_count = node.get("comment_count", 0)
                
                if follower_count > 0:
                    post_er = ((likes_count + comments_count) / follower_count) * 100
                else:
                    post_er = 0
                latest_posts_er_data[f"Post{i+1}"] = round(post_er, 2)
                total_latest_er += post_er
            
            metrics['latest_posts_er'] = latest_posts_er_data
            if len(latest_posts) > 0:
                metrics['avg_latest_posts_er'] = round(total_latest_er / len(latest_posts), 3)
            else:
                metrics['avg_latest_posts_er'] = 0
        
        except Exception as e:
            self.log_message(f"Error calculating engagement metrics: {str(e)}", level="WARNING")
        
        return metrics

    def detect_paid_partnerships(self, posts_data, bio_text=""):
        """Enhanced paid partnership detection with detailed analysis."""
        partnership_data = {
            'has_paid_partnerships': False,
            'total_sponsored_posts': 0,
            'sponsor_frequency': {},
            'sponsored_posts': [],
            'collaboration_timeline': [],
            'avg_sponsored_engagement': 0,
            'avg_organic_engagement': 0,
            'engagement_comparison': {},
            'affiliate_links': [],
            'brand_mentions': [],
            'collaboration_score': 0
        }
        
        try:
            # Partnership indicators in bio
            bio_indicators = [
                'brand ambassador', 'sponsored', 'partnership', 'collab', 'pr',
                'affiliate', 'discount code', 'promo code', 'brand partner',
                'collaboration', 'paid partnership', '#ad', '#sponsored'
            ]
            
            bio_lower = bio_text.lower() if bio_text else ""
            bio_partnerships = any(indicator in bio_lower for indicator in bio_indicators)
            
            # Analyze posts for partnerships
            edges = posts_data.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection", {}).get("edges", [])
            
            sponsored_posts = []
            organic_posts = []
            sponsors = {}
            brand_mentions = set()
            
            for edge in edges:
                node = edge.get("node", {})
                caption = node.get("caption", {}).get("text", "") if node.get("caption") else ""
                
                # Check for partnership indicators
                is_sponsored = False
                partnership_indicators = [
                    '#ad', '#sponsored', '#partnership', '#collab', '#pr',
                    'paid partnership', 'sponsored by', 'in collaboration with',
                    'thanks to', 'gifted by', 'promo code', 'discount code',
                    'affiliate link', 'brand ambassador', '#brandambassador'
                ]
                
                caption_lower = caption.lower()
                for indicator in partnership_indicators:
                    if indicator in caption_lower:
                        is_sponsored = True
                        break
                
                # Check for sponsor tags (if available in data)
                sponsor_tags = node.get('sponsor_tags', []) if node else []
                if sponsor_tags:
                    is_sponsored = True
                    for sponsor in sponsor_tags:
                        sponsor_username = sponsor.get('username', '')
                        if sponsor_username:
                            sponsors[sponsor_username] = sponsors.get(sponsor_username, 0) + 1
                
                # Extract brand mentions from hashtags and mentions
                hashtags = re.findall(r'#(\w+)', caption)
                mentions = re.findall(r'@(\w+)', caption)
                
                for mention in mentions:
                    if mention.lower() not in ['instagram', 'facebook', 'twitter']:
                        brand_mentions.add(mention)
                
                post_data = {
                    'id': node.get('id', ''),
                    'code': node.get('code', ''),
                    'shortcode': node.get('shortcode', ''),
                    'caption': caption,
                    'like_count': node.get('like_count', 0),
                    'comment_count': node.get('comment_count', 0),
                    'taken_at': node.get('taken_at', 0),
                    'is_video': node.get('is_video', False),
                    'hashtags': hashtags,
                    'mentions': mentions,
                    'sponsor_tags': sponsor_tags,
                    'engagement_rate': 0
                }
                
                # Calculate engagement for this post
                total_engagement = post_data['like_count'] + post_data['comment_count']
                post_data['total_engagement'] = total_engagement
                
                if is_sponsored:
                    sponsored_posts.append(post_data)
                else:
                    organic_posts.append(post_data)
            
            # Calculate metrics
            partnership_data['total_sponsored_posts'] = len(sponsored_posts)
            partnership_data['has_paid_partnerships'] = len(sponsored_posts) > 0 or bio_partnerships
            partnership_data['sponsored_posts'] = sponsored_posts
            partnership_data['sponsor_frequency'] = sponsors
            partnership_data['brand_mentions'] = list(brand_mentions)
            
            # Calculate engagement comparison
            if sponsored_posts:
                sponsored_engagement = [p['total_engagement'] for p in sponsored_posts]
                partnership_data['avg_sponsored_engagement'] = statistics.mean(sponsored_engagement) if sponsored_engagement else 0
            
            if organic_posts:
                organic_engagement = [p['total_engagement'] for p in organic_posts]
                partnership_data['avg_organic_engagement'] = statistics.mean(organic_engagement) if organic_engagement else 0
            
            # Engagement comparison
            if partnership_data['avg_sponsored_engagement'] > 0 and partnership_data['avg_organic_engagement'] > 0:
                ratio = partnership_data['avg_sponsored_engagement'] / partnership_data['avg_organic_engagement']
                partnership_data['engagement_comparison'] = {
                    'sponsored_vs_organic_ratio': round(ratio, 2),
                    'sponsored_performs_better': ratio > 1.0
                }
            
            # Create collaboration timeline
            collaboration_timeline = []
            for post in sponsored_posts:
                if post.get('taken_at'):
                    collaboration_timeline.append({
                        'date': datetime.datetime.fromtimestamp(post['taken_at']).strftime('%Y-%m-%d'),
                        'post_id': post['id'],
                        'sponsors': [tag.get('username') for tag in post.get('sponsor_tags', []) if isinstance(tag, dict)],
                        'engagement': post['total_engagement']
                    })
            
            partnership_data['collaboration_timeline'] = sorted(collaboration_timeline, key=lambda x: x['date'], reverse=True)
            
            # Calculate collaboration score (0-100)
            score_factors = [
                min(len(sponsored_posts) * 10, 40),  # Number of sponsored posts (max 40 points)
                min(len(sponsors) * 15, 30),  # Number of unique sponsors (max 30 points)
                20 if bio_partnerships else 0,  # Bio mentions partnerships (20 points)
                10 if partnership_data['engagement_comparison'].get('sponsored_performs_better', False) else 0  # Performance (10 points)
            ]
            partnership_data['collaboration_score'] = sum(score_factors)
            
        except Exception as e:
            self.log_message(f"Error detecting paid partnerships: {str(e)}", level="WARNING")
        
        return partnership_data

    def extract_basic_info(self, user_info: dict) -> dict:
        """
        Extracts and formats basic creator information into a single string.
        
        Args:
            user_info: Dictionary containing user profile data.
        
        Returns:
            A formatted string with the creator's username, full name, follower count,
            biography, and profile picture URL.
        """
        user_data = user_info.get('data', {}).get('user', {})
        username = user_data.get('username', '')
        follower_count = user_data.get('follower_count', '')
        full_name = user_data.get('full_name', '')
        biography = user_data.get('biography', '')
        category = user_data.get('category','')
        profile_picture = f"https://assets.veelapp.com/{username}.jpg" if username != '' else ''
        
        return {
            'username': username,
            'follower_count': follower_count,
            'full_name': full_name,
            'biography': biography,
            'profile_picture': profile_picture,
            'category': category
        }

    def identify_gender(self, user_info: dict) -> str:
        """
        Identify gender based on pronouns field and other indicators in user profile.
        
        Args:
            user_info: Dictionary containing user profile data
        
        Returns:
            String indicating gender: 'Male', 'Female', 'Non-binary', 'Unknown'
        """
        user_data = user_info.get('data', {}).get('user', {})
        
        # Check pronouns field first (most reliable)
        pronouns = user_data.get('pronouns', [])
        if pronouns and isinstance(pronouns, list):
            for pronoun_obj in pronouns:
                if isinstance(pronoun_obj, dict):
                    pronoun_text = pronoun_obj.get('pronoun', '').lower().strip()
                    if pronoun_text:
                        if pronoun_text in ['she/her', 'she', 'her']:
                            return 'Female'
                        elif pronoun_text in ['he/him', 'he', 'him']:
                            return 'Male'
                        elif pronoun_text in ['they/them', 'they', 'them', 'ze/zir', 'xe/xem', 'it/its']:
                            return 'Non-binary'
                elif isinstance(pronoun_obj, str):
                    pronoun_text = pronoun_obj.lower().strip()
                    if pronoun_text in ['she/her', 'she', 'her']:
                        return 'Female'
                    elif pronoun_text in ['he/him', 'he', 'him']:
                        return 'Male'
                    elif pronoun_text in ['they/them', 'they', 'them', 'ze/zir', 'xe/xem', 'it/its']:
                        return 'Non-binary'
        
        # If no pronouns, try to infer from biography and full name
        biography = user_data.get('biography', '').lower() if user_data.get('biography') else ''
        full_name = user_data.get('full_name', '').lower() if user_data.get('full_name') else ''
        username = user_data.get('username', '').lower() if user_data.get('username') else ''
        
        # Combine all text for analysis
        all_text = f"{biography} {full_name} {username}"
        
        # Look for gender indicators in text
        female_indicators = [
            'she/her', 'she', 'her', 'woman', 'girl', 'female', 'lady', 'mom', 'mother', 
            'wife', 'daughter', 'sister', 'girlfriend', 'actress', 'queen', 'princess',
            'mama', 'mum', 'mummy', 'mommy', 'mrs', 'ms', 'miss'
        ]
        
        male_indicators = [
            'he/him', 'he', 'him', 'man', 'boy', 'male', 'guy', 'dad', 'father',
            'husband', 'son', 'brother', 'boyfriend', 'actor', 'king', 'prince',
            'papa', 'daddy', 'mr'
        ]
        
        non_binary_indicators = [
            'they/them', 'them', 'they', 'non-binary', 'nonbinary', 'nb', 'enby',
            'genderfluid', 'genderqueer', 'agender', 'ze/zir', 'xe/xem'
        ]
        
        # Count indicators
        female_score = sum(1 for indicator in female_indicators if indicator in all_text)
        male_score = sum(1 for indicator in male_indicators if indicator in all_text)
        non_binary_score = sum(1 for indicator in non_binary_indicators if indicator in all_text)
        
        # Determine gender based on highest score
        max_score = max(female_score, male_score, non_binary_score)
        if max_score == 0:
            return 'Unknown'
        elif female_score == max_score:
            return 'Female'
        elif male_score == max_score:
            return 'Male'
        else:
            return 'Non-binary'

    def extract_social_links(self, user_info: dict) -> dict:
        """
        Extract TikTok, YouTube, and Linktree URLs from bio_links.
        Neglects all other link types.
        
        Args:
            user_info: Dictionary containing user profile data
        
        Returns:
            Dictionary with extracted social media links
        """
        # Get bio_links from nested user data structure
        user_data = user_info.get('data', {}).get('user', {})
        bio_links = user_data.get('bio_links', [])
        
        extracted_links = {
            'tiktok': None,
            'youtube': None,
            'linktree': None
        }
        
        # Define patterns to match the platforms we want
        platform_patterns = {
            'tiktok': ['tiktok.com', 'tiktok.app'],
            'youtube': ['youtube.com', 'youtu.be'],
            'linktree': ['linktr.ee']
        }
        
        for link_obj in bio_links:
            if not isinstance(link_obj, dict):
                continue
                
            url = link_obj.get('url', '')
            if not url:
                continue
            
            url_lower = url.lower()
            
            # Check each platform pattern
            for platform, patterns in platform_patterns.items():
                for pattern in patterns:
                    if pattern in url_lower and extracted_links[platform] is None:
                        extracted_links[platform] = url
                        break
        
        return extracted_links

    def extract_creator_pricing(self, user_info: dict, posts: List[dict]) -> dict:
        """
        Identifies the creator type and tier and then calculates various performance creator_pricing_metrics
        based on the provided guidelines.
        """
        ugc_keywords = [
            'ugc', 'ugccreator', 'ugc creator', 'user generated content',
            'user-generated content', 'content creator', 'brand creator',
            'ugc content', 'product creator'
        ]
        
        user_data = user_info.get('data', {}).get('user', {})
        username = user_data.get('username', '').lower()
        fullname = user_data.get('full_name', '').lower()
        biography = user_data.get('biography', '').lower()
        follower_count = user_data.get('follower_count', 0)
        
        creator_type = "Social Media Influencer" # Default
        
        for text in [fullname, username, biography]:
            if any(keyword in text for keyword in ugc_keywords):
                creator_type = "UGC Creator"
                break
        
        if creator_type != "UGC Creator":
            for post in posts:
                try:
                    caption_text = post.get('node', {}).get('caption', {}).get('text', '')
                    caption_lower = caption_text.lower()
                    if any(keyword in caption_lower or f'#{keyword.replace(" ", "")}' in caption_lower for keyword in ugc_keywords):
                        creator_type = "UGC Creator"
                        break
                except (AttributeError, TypeError, KeyError):
                    continue
        
        tier = "Unknown"
        
        if creator_type == "Social Media Influencer" and follower_count < 1000:
            creator_type = "UGC Creator"
            tier = "Beginner"
            
        elif creator_type == "UGC Creator":
            if follower_count < 1000:
                tier = "Beginner"
            else:
                tier = "Experienced"
        
        elif creator_type == "Social Media Influencer":
            if follower_count < 10000:
                tier = "1K-10K"
            elif follower_count < 50000:
                tier = "10K-50K"
            elif follower_count < 500000:
                tier = "50K-500K"
            else:
                tier = "500K-1M+"
        
        creator_pricing_metrics = {
            'estimated_roi': 'N/A',
            'impressions_visibility': 'N/A',
            'time_15_seconds': 'N/A',
            'time_30_seconds': 'N/A',
            'time_60_seconds': 'N/A',
            'time_1_to_5_minutes': 'N/A',
            'time_greater_than_5_minutes': 'N/A'
        }

        if creator_type == "UGC Creator":
            if tier == "Beginner":
                creator_pricing_metrics['estimated_roi'] = '3×–6×'
                creator_pricing_metrics['impressions_visibility'] = '30K'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 100)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 100)
                creator_pricing_metrics['time_60_seconds'] = 100
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 100)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 100)
            elif tier == "Experienced":
                creator_pricing_metrics['estimated_roi'] = '5×–9×'
                creator_pricing_metrics['impressions_visibility'] = '85K'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 300)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 300)
                creator_pricing_metrics['time_60_seconds'] = 300
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 300)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 300)
        
        elif creator_type == "Social Media Influencer":
            if tier == "1K-10K":
                creator_pricing_metrics['estimated_roi'] = '6×–10×'
                creator_pricing_metrics['impressions_visibility'] = '165K'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 150)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 150)
                creator_pricing_metrics['time_60_seconds'] = 150
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 150)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 150)
            elif tier == "10K-50K":
                creator_pricing_metrics['estimated_roi'] = '6×–10×'
                creator_pricing_metrics['impressions_visibility'] = '300K'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 500)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 500)
                creator_pricing_metrics['time_60_seconds'] = 500
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 500)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 500)
            elif tier == "50K-500K":
                creator_pricing_metrics['estimated_roi'] = '4×–7×'
                creator_pricing_metrics['impressions_visibility'] = '1M'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 2500)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 2500)
                creator_pricing_metrics['time_60_seconds'] = 2500
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 2500)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 2500)
            elif tier == "500K-1M+":
                creator_pricing_metrics['estimated_roi'] = '3×–6×'
                creator_pricing_metrics['impressions_visibility'] = '3.2M'
                creator_pricing_metrics['time_15_seconds'] = round(0.4 * 4000)
                creator_pricing_metrics['time_30_seconds'] = round(0.6 * 4000)
                creator_pricing_metrics['time_60_seconds'] = 4000
                creator_pricing_metrics['time_1_to_5_minutes'] = round(1.333 * 4000)
                creator_pricing_metrics['time_greater_than_5_minutes'] = round(2 * 4000)
            
        return {
            'creator_type': creator_type,
            'tier': tier,
            'creator_pricing_metrics': creator_pricing_metrics
        }

    def identify_niche(self, user_info: dict) -> dict:
        """Identify the creator's overall niche based on keywords in the biography, username, and full name."""
        # Define niche categories with relevant keywords
        niche_categories = {
            "Fashion & Style": ["fashion", "style", "outfit", "clothing", "model", "dress", "accessories", "fashionista", "ootd", "stylist", "boutique", "wardrobe", "trend", "chic"],
            "Beauty": ["makeup", "skincare", "beauty", "cosmetics", "haircare", "nails", "glam", "makeupartist", "beautician", "mua", "skincare", "beautyblogger", "makeover", "cosmetic"],
            "Lifestyle": ["lifestyle", "life", "daily", "routine", "inspiration", "motivation", "blogger", "lifestyleblogger", "living", "vibes", "mindful", "wellness"],
            "Fitness": ["fitness", "workout", "gym", "exercise", "health", "training", "muscle", "fit", "fitnessmotivation", "trainer", "bodybuilding", "crossfit", "yoga", "pilates"],
            "Health": ["health", "wellness", "nutrition", "diet", "healthy", "mindfulness", "meditation", "nutritionist", "dietitian", "wellbeing", "mental", "holistic"],
            "Food": ["food", "cooking", "recipe", "chef", "foodie", "cuisine", "baking", "delicious", "yummy", "foodblogger", "culinary", "restaurant", "eats", "tasty", "kitchen"],
            "Travel": ["travel", "wanderlust", "adventure", "explore", "tourism", "vacation", "trip", "journey", "destination", "traveler", "backpacker", "nomad", "wanderer", "explorer"],
            "Technology": ["technology", "tech", "gadget", "device", "software", "app", "smartphone", "computer", "digital", "innovation", "startup", "coding", "developer", "geek"],
            "Gaming": ["gaming", "gamer", "videogames", "game", "esports", "playstation", "xbox", "nintendo", "streamer", "twitch", "console", "pc", "mobile", "rpg"],
            "Entertainment": ["entertainment", "movie", "film", "tv", "television", "cinema", "streaming", "actor", "actress", "celebrity", "show", "series", "netflix"],
            "Comedy": ["comedy", "funny", "humor", "laugh", "joke", "prank", "skit", "comedian", "hilarious", "meme", "viral", "comic", "witty"],
            "Education": ["education", "learning", "school", "knowledge", "teach", "study", "student", "lesson", "teacher", "tutor", "academic", "university", "college", "learn"],
            "Business": ["business", "entrepreneur", "marketing", "startup", "success", "money", "ceo", "founder", "corporate", "leadership", "boss", "hustle", "businesswoman"],
            "Finance": ["finance", "investing", "stocks", "cryptocurrency", "money", "financial", "wealth", "investor", "trader", "bitcoin", "crypto", "forex", "portfolio"],
            "Art & Design": ["art", "artist", "drawing", "painting", "creative", "design", "illustration", "designer", "painter", "sculptor", "gallery", "artwork", "canvas"],
            "Music": ["music", "musician", "song", "singer", "artist", "band", "concert", "producer", "dj", "songwriter", "vocalist", "guitarist", "pianist", "rapper"],
            "Dance": ["dance", "dancer", "choreography", "ballet", "hiphop", "contemporary", "ballroom", "choreographer", "studio", "performance", "movement"],
            "Sports": ["sports", "athlete", "basketball", "football", "soccer", "baseball", "tennis", "coach", "player", "team", "competition", "championship", "olympics"],
            "Pets & Animals": ["pets", "dog", "cat", "animal", "puppy", "kitten", "wildlife", "veterinarian", "petcare", "rescue", "adoption", "dogtrainer", "animallover"],
            "Family & Parenting": ["family", "parenting", "mom", "dad", "children", "kids", "baby", "mother", "father", "parent", "motherhood", "fatherhood", "parenting", "toddler"]
        }
        
        # Get user data from the nested structure
        user_data = user_info.get('data', {}).get('user', {})
        biography = user_data.get('biography', '') or ''
        username = user_data.get('username', '') or ''
        full_name = user_data.get('full_name', '') or ''
        
        # Combine all text sources for analysis
        all_text_sources = {
            'biography': biography,
            'username': username,
            'full_name': full_name
        }
        
        # Extract all keywords from all categories
        all_keywords = set()
        for keywords in niche_categories.values():
            all_keywords.update(keywords)
        
        # Process each text source and track where keywords were found
        all_matched_keywords = []
        keyword_sources = {}  # Track which source each keyword came from
        total_keyword_counts = {}
        
        for source_name, text in all_text_sources.items():
            if not text:
                continue
                
            # Clean and process text
            # For username, also split on underscores and remove special characters
            if source_name == 'username':
                # Remove leading/trailing underscores and split on underscores
                clean_text = text.strip('_').replace('_', ' ').replace('.', ' ')
                words = [word.strip().lower() for word in clean_text.split() if word and len(word) > 1]
            else:
                # For biography and full_name, use standard word splitting
                words = [word.strip().lower() for word in text.replace(',', ' ').replace('\n', ' ').split() if word]
            
            # Filter words that match niche keywords
            matched_keywords = [word for word in words if word in all_keywords]
            
            # Track matches by source
            for keyword in matched_keywords:
                all_matched_keywords.append(keyword)
                if keyword not in keyword_sources:
                    keyword_sources[keyword] = []
                keyword_sources[keyword].append(source_name)
                total_keyword_counts[keyword] = total_keyword_counts.get(keyword, 0) + 1
        
        # Score each niche category with weighted scoring
        # Username and full_name get higher weight since they're more intentional
        source_weights = {
            'username': 2.0,    # Higher weight for username
            'full_name': 1.0,   # Medium weight for full name  
            'biography': 1.5    # Standard weight for biography
        }
        
        niche_scores = {category: 0 for category in niche_categories}
        detailed_matches = {category: [] for category in niche_categories}
        
        for keyword, count in total_keyword_counts.items():
            for category, keywords in niche_categories.items():
                if keyword in keywords:
                    # Apply weighted scoring based on where the keyword was found
                    sources_for_keyword = keyword_sources[keyword]
                    weighted_score = 0
                    for source in sources_for_keyword:
                        weighted_score += source_weights.get(source, 1.0)
                
                    niche_scores[category] += weighted_score * count
                    detailed_matches[category].append({
                        'keyword': keyword, 
                        'count': count, 
                        'sources': sources_for_keyword,
                        'weighted_score': weighted_score * count
                    })
        
        # Calculate distribution percentages
        total_score = sum(niche_scores.values()) or 1  # Avoid division by zero
        distribution = {category: round(score / total_score * 100, 1) for category, score in niche_scores.items() if score > 0}
        
        # Filter out negligible categories (less than 2%)
        significant_distribution = {k: v for k, v in distribution.items() if v >= 2}
        
        # Determine overall niche (highest scored category)
        sorted_niches = sorted(niche_scores.items(), key=lambda x: x[1], reverse=True)
        overall_niche = sorted_niches[0][0] if sorted_niches and sorted_niches[0][1] > 0 else None
        
        # Confidence scores - normalize to 0-100 scale
        confidence_scores = {}
        max_score = sorted_niches[0][1] if sorted_niches and sorted_niches[0][1] > 0 else 1
        for category in niche_categories:
            score = niche_scores.get(category, 0)
            confidence_scores[category] = min(100, int((score / max_score) * 100))
        
        # Create source analysis
        source_analysis = {}
        for source_name, text in all_text_sources.items():
            if text:
                source_words = []
                if source_name == 'username':
                    clean_text = text.strip('_').replace('_', ' ').replace('.', ' ')
                    source_words = [word.strip().lower() for word in clean_text.split() if word and len(word) > 1]
                else:
                    source_words = [word.strip().lower() for word in text.replace(',', ' ').replace('\n', ' ').split() if word]
            
                matched_in_source = [word for word in source_words if word in all_keywords]
                source_analysis[source_name] = {
                    'text': text,
                    'matched_keywords': matched_in_source,
                    'match_count': len(matched_in_source)
                }
        
        return {
            "overall_niche": overall_niche,
            "distribution": significant_distribution,
            "confidence_scores": confidence_scores,
            "matched_keywords": all_matched_keywords,
            "keyword_sources": keyword_sources,
            "source_analysis": source_analysis,
            "detailed_matches": detailed_matches,
            "niche_scores": dict(sorted_niches),  # Include raw scores for debugging
            "biography_analyzed": biography,
            "username_analyzed": username,
            "full_name_analyzed": full_name
        }

    def extract_ugc_examples(self, posts: List[dict]) -> str:
        """Extract UGC video examples from posts with collaboration indicators (clips only)."""
        if not posts:
            return ""
        
        ugc_codes = []
        recent_threshold = 90  # days
        today = datetime.datetime.now()
        recent_cutoff = today - datetime.timedelta(days=recent_threshold)
        
        # Get username for comparison
        uname = None
        try:
            if posts and len(posts) > 0:
                first_post = posts[0]
                if first_post and isinstance(first_post, dict):
                    node = first_post.get("node", {})
                    if node:
                        user_data = node.get("user", {})
                        if user_data:
                            uname = user_data.get("username")
        except (AttributeError, TypeError, IndexError):
            pass
        
        # Check for paid partnerships first (highest priority) - only clips
        for post in posts:
            try:
                if not post or not isinstance(post, dict):
                    continue
                    
                node = post.get('node', {})
                if not node:
                    continue
                
                # Check if it's a clip (video content only)
                if node.get('product_type') != 'clips':
                    continue
                
                # Check if it's a paid partnership
                if node.get('is_paid_partnership') is True:
                    code = node.get('code')
                    if code and len(ugc_codes) < 3:
                        ugc_codes.append(code)
                        
            except (AttributeError, TypeError, KeyError):
                continue
        
        # If we don't have 3 examples yet, check for posts with #ad or #collab - only clips
        if len(ugc_codes) < 3:
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                    
                    # Check if it's a clip (video content only)
                    if node.get('product_type') != 'clips':
                        continue
                    
                    caption_obj = node.get('caption')
                    caption = ''
                    if caption_obj and isinstance(caption_obj, dict):
                        caption = caption_obj.get('text', '') or ''
                    
                    if caption and isinstance(caption, str):
                        caption_lower = caption.lower()
                        if '#ad' in caption_lower or '#collab' in caption_lower:
                            code = node.get('code')
                            if code and code not in ugc_codes and len(ugc_codes) < 3:
                                ugc_codes.append(code)
                                
                except (AttributeError, TypeError, KeyError):
                    continue
        
        # If still need more examples, check posts with different owners (collaborations) - only clips
        if len(ugc_codes) < 3 and uname:
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                    
                    # Check if it's a clip (video content only)
                    if node.get('product_type') != 'clips':
                        continue
                    
                    owner = node.get('owner', {})
                    if owner and isinstance(owner, dict):
                        post_owner_username = owner.get('username')
                        if post_owner_username and post_owner_username != uname:
                            code = node.get('code')
                            if code and code not in ugc_codes and len(ugc_codes) < 3:
                                ugc_codes.append(code)
                                
                except (AttributeError, TypeError, KeyError):
                    continue
        
        # If still need more, check for coauthor_producers - only clips
        if len(ugc_codes) < 3 and uname:
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                    
                    # Check if it's a clip (video content only)
                    if node.get('product_type') != 'clips':
                        continue
                    
                    coauthor_producers = node.get('coauthor_producers')
                    if coauthor_producers and isinstance(coauthor_producers, list):
                        for coauthor in coauthor_producers:
                            if coauthor and isinstance(coauthor, dict):
                                coauthor_username = coauthor.get("username")
                                if coauthor_username and coauthor_username != uname:
                                    code = node.get('code')
                                    if code and code not in ugc_codes and len(ugc_codes) < 3:
                                        ugc_codes.append(code)
                                        break
                                        
                except (AttributeError, TypeError, KeyError):
                    continue
        
        # Convert codes to Instagram URLs and join with |
        if ugc_codes:
            urls = [f"https://www.instagram.com/p/{code}" for code in ugc_codes]
            return " | ".join(urls)
        
        return ""

    def identify_collaborations(self, posts: List[dict]) -> Dict:
        """Identify potential brand collaborations from posts using specified logic."""
        if not posts:
            return {
                'status': None,
                'total_collaborations': 0,
                'recent_collaborations': 0,
                'all_collaborations': [],
                'ugc_examples': ""
            }
        
        uname = None
        try:
            if posts and len(posts) > 0:
                first_post = posts[0]
                if first_post and isinstance(first_post, dict):
                    node = first_post.get("node", {})
                    if node:
                        user_data = node.get("user", {})
                        if user_data:
                            uname = user_data.get("username")
        except (AttributeError, TypeError, IndexError):
            pass    

        final_status = None
        all_collabs = []
        recent_brands = []
        recent_threshold = 300  # days
        today = datetime.datetime.now()
        recent_cutoff = today - datetime.timedelta(days=recent_threshold)
        seen_collabs = set()

        # Priority: explicit paid partnership flag
        for post in posts:
            try:
                if not post or not isinstance(post, dict):
                    continue
                    
                node = post.get('node', {})
                if not node:
                    continue
                    
                if node.get('is_paid_partnership') is True:
                    final_status = "Active"
                    caption_obj = node.get('caption')
                    caption = ''
                    if caption_obj and isinstance(caption_obj, dict):
                        caption = caption_obj.get('text', '') or ''
                    
                    taken_at = node.get('taken_at')
                    is_recent = False
                    if taken_at:
                        try:
                            post_date = datetime.datetime.fromtimestamp(taken_at)
                            is_recent = post_date > recent_cutoff
                        except (ValueError, TypeError):
                            pass
                    
                    if caption and isinstance(caption, str):
                        mentions = re.findall(r'@([A-Za-z0-9._]+)', caption)
                        for mention in mentions:
                            if len(mention) < 3 or mention.lower() in ['the', 'and', 'for', 'from', 'with', 'this', 'that', 'have', 'has', 'her', 'his', 'our', 'my', 'your', 'their', 'its', 'as', 'at', 'by', 'to', 'in', 'on', 'of', 'or', 'if']:
                                continue
                            if mention not in seen_collabs:
                                all_collabs.append({
                                    'name': mention,
                                    'count': 1,
                                    'is_recent': is_recent,
                                    'source': 'paid_partnership'
                                })
                                seen_collabs.add(mention)
                                if is_recent:
                                    recent_brands.append({'name': mention, 'source': 'mention'})
                    
                    break
            except (AttributeError, TypeError, KeyError):
                continue

        # Check owners and coauthors
        for post in posts:
            try:
                if not post or not isinstance(post, dict):
                    continue
                    
                node = post.get('node', {})
                if not node:
                    continue
                    
                taken_at = node.get('taken_at')
                is_recent = False
                if taken_at:
                    try:
                        post_date = datetime.datetime.fromtimestamp(taken_at)
                        is_recent = post_date > recent_cutoff
                    except (ValueError, TypeError):
                        pass

                owner = node.get('owner', {})
                if owner and isinstance(owner, dict):
                    post_owner_username = owner.get('username')
                    if post_owner_username and post_owner_username != uname and post_owner_username not in seen_collabs:
                        all_collabs.append({
                            'name': post_owner_username,
                            'count': 1,
                            'is_recent': is_recent,
                            'source': 'owner'
                        })
                        seen_collabs.add(post_owner_username)
                        if is_recent:
                            recent_brands.append({'name': post_owner_username, 'source': 'owner'})

                coauthor_producers = node.get('coauthor_producers')
                if coauthor_producers and isinstance(coauthor_producers, list):
                    for coauthor in coauthor_producers:
                        if coauthor and isinstance(coauthor, dict):
                            coauthor_username = coauthor.get("username")
                            if coauthor_username and coauthor_username != uname and coauthor_username not in seen_collabs:
                                all_collabs.append({
                                    'name': coauthor_username,
                                    'count': 1,
                                    'is_recent': is_recent,
                                    'source': 'coauthor'
                                })
                                seen_collabs.add(coauthor_username)
                                if is_recent:
                                    recent_brands.append({'name': coauthor_username, 'source': 'coauthor'})

            except (AttributeError, TypeError, KeyError):
                continue

        # If not explicit, look for hashtags/mentions indicating collabs
        if final_status is None:
            status_hashtags = ['ad', 'collab']
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                        
                    caption_obj = node.get('caption')
                    caption = ''
                    if caption_obj and isinstance(caption_obj, dict):
                        caption = caption_obj.get('text', '') or ''
                        
                    taken_at = node.get('taken_at')
                    is_recent = False
                    if taken_at:
                        try:
                            post_date = datetime.datetime.fromtimestamp(taken_at)
                            is_recent = post_date > recent_cutoff
                        except (ValueError, TypeError):
                            pass
                    
                    if caption and isinstance(caption, str):
                        caption_lower = caption.lower()
                        for tag in status_hashtags:
                            if f'#{tag}' in caption_lower:
                                final_status = "Active"
                                mentions = re.findall(r'@([A-Za-z0-9._]+)', caption)
                                for mention in mentions:
                                    if len(mention) < 3 or mention.lower() in ['the', 'and', 'for', 'from', 'with', 'this', 'that', 'have', 'has', 'her', 'his', 'our', 'my', 'your', 'their', 'its', 'as', 'at', 'by', 'to', 'in', 'on', 'of', 'or', 'if']:
                                        continue
                                    if mention not in seen_collabs:
                                        all_collabs.append({
                                            'name': mention,
                                            'count': 1,
                                            'is_recent': is_recent,
                                            'source': 'tag'
                                        })
                                        seen_collabs.add(mention)
                                        if is_recent:
                                            recent_brands.append({'name': mention, 'source': 'mention'})
                                break
                    if final_status == "Active":
                        break
                except (AttributeError, TypeError, KeyError):
                    continue

        # Fallback checks for owners/coauthors marking active collaborations
        if final_status is None and uname:
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                        
                    owner = node.get('owner', {})
                    if owner and isinstance(owner, dict):
                        post_owner_username = owner.get('username')
                        if post_owner_username and post_owner_username != uname:
                            final_status = "Active"
                            break
                except (AttributeError, TypeError, KeyError):
                    continue

        if final_status is None and uname:
            for post in posts:
                try:
                    if not post or not isinstance(post, dict):
                        continue
                        
                    node = post.get('node', {})
                    if not node:
                        continue
                    
                    coauthor_producers = node.get('coauthor_producers')
                    if coauthor_producers and isinstance(coauthor_producers, list):
                        for coauthor in coauthor_producers:
                            if coauthor and isinstance(coauthor, dict):
                                coauthor_username = coauthor.get("username")
                                if coauthor_username and coauthor_username != uname:
                                    final_status = "Active"
                                    break
                        if final_status == "Active":
                            break
                except (AttributeError, TypeError, KeyError):
                    continue

        # Extract UGC examples
        ugc_examples = self.extract_ugc_examples(posts)

        collaboration_info = {
            'status': final_status,
            'total_collaborations': len(all_collabs),
            'recent_collaborations': len(recent_brands),
            'all_collaborations': all_collabs,
            'ugc_examples': ugc_examples
        }
        return collaboration_info

    def calculate_top_post_er(self, post_info: dict, user_info: dict) -> tuple:
        """
        Analyzes posts from the last 3 months, calculates engagement scores,
        finds the top 6 posts, and computes their average engagement rate (ER).
        """
        # Get follower count from user_info
        followers = user_info.get('data', {}).get('user', {}).get('follower_count', 0)

        if not followers:
            print(f"{Fore.RED}Cannot calculate ER: Follower count is zero.{Style.RESET_ALL}")
            return 0, [], 0

        # Get posts from the last 3 months
        three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
        three_months_ago_unix = int(three_months_ago.timestamp())

        all_posts = post_info.get('data', {}).get('xdt_api__v1__feed__user_timeline_graphql_connection', {}).get('edges', [])

        recent_posts_with_scores = []
        total_last_three_months_posts = 0

        for post in all_posts:
            node = post.get('node', {})
            post_time = node.get('taken_at', 0)

            if post_time >= three_months_ago_unix:
                total_last_three_months_posts += 1
                likes = node.get('like_count', 0)
                comments = node.get('comment_count', 0)

                # Calculate the engagement score as per request: likes + (5 * comments)
                interaction_score = likes + (5 * comments)

                # Calculate individual ER for this post
                individual_er = ((likes + (5 * comments)) / followers) * 100

                recent_posts_with_scores.append({
                    'interaction_score': interaction_score,
                    'likes': likes,
                    'comments': comments,
                    'engagement_rate': round(individual_er, 2),
                    'post_code': node.get('code', ''),
                    'taken_at': datetime.datetime.fromtimestamp(post_time).strftime('%Y-%m-%d') if post_time else ''
                })

        # Sort posts in descending order by engagement score
        sorted_posts = sorted(recent_posts_with_scores, key=lambda p: p['interaction_score'], reverse=True)

        # Get the top 6 posts (or fewer if less than 6 were found)
        top_posts = sorted_posts[:6]

        # Calculate the average ER of the top posts
        avg_er = sum(post['engagement_rate'] for post in top_posts) / len(top_posts) if top_posts else 0

        return total_last_three_months_posts, top_posts, round(avg_er, 2)

    def extract_hashtags_and_mentions(self, posts: List[dict], limit: int = 10) -> Dict:
        """
        Extract hashtags and mentions from posts from the last 90 days.
        """
        if not posts:
            return {
                'hashtags': {},
                'mentions': {},
                'total_posts_analyzed': 0,
                'date_range': 'No posts found'
            }
        
        # Calculate 90 days ago from today
        ninety_days_ago = datetime.datetime.now() - datetime.timedelta(days=90)
        ninety_days_ago_unix = int(ninety_days_ago.timestamp())
        
        hashtag_counts = {}
        mention_counts = {}
        posts_analyzed = 0
        
        for post in posts:
            try:
                if not post or not isinstance(post, dict):
                    continue
                    
                node = post.get('node', {})
                if not node:
                    continue
                
                # Check if post is from last 90 days
                taken_at = node.get('taken_at', 0)
                if taken_at < ninety_days_ago_unix:
                    continue
                
                posts_analyzed += 1
                
                # Get caption text
                caption_obj = node.get('caption')
                if not caption_obj or not isinstance(caption_obj, dict):
                    continue
                    
                caption_text = caption_obj.get('text', '')
                if not caption_text or not isinstance(caption_text, str):
                    continue
                
                # Extract hashtags (everything after # until whitespace or end)
                hashtags = re.findall(r'#([A-Za-z0-9_]+)', caption_text)
                for hashtag in hashtags:
                    hashtag_lower = hashtag.lower()
                    hashtag_counts[hashtag_lower] = hashtag_counts.get(hashtag_lower, 0) + 1
                
                # Extract mentions (everything after @ until whitespace or end)
                mentions = re.findall(r'@([A-Za-z0-9._]+)', caption_text)
                for mention in mentions:
                    # Filter out very short mentions or common words
                    if len(mention) >= 3 and mention.lower() not in ['the', 'and', 'for', 'from', 'with', 'this', 'that', 'have', 'has', 'her', 'his', 'our', 'my', 'your', 'their', 'its', 'as', 'at', 'by', 'to', 'in', 'on', 'of', 'or', 'if']:
                        mention_lower = mention.lower()
                        mention_counts[mention_lower] = mention_counts.get(mention_lower, 0) + 1
                        
            except (AttributeError, TypeError, KeyError) as e:
                continue
        
        # Sort by frequency and limit to top results
        top_hashtags = dict(sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:limit])
        top_mentions = dict(sorted(mention_counts.items(), key=lambda x: x[1], reverse=True)[:limit])
        
        # Create date range string
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        ninety_days_ago_str = ninety_days_ago.strftime('%Y-%m-%d')
        date_range = f"{ninety_days_ago_str} to {today_str}"
        
        return {
            'hashtags': top_hashtags,
            'mentions': top_mentions,
            'total_posts_analyzed': posts_analyzed,
            'date_range': date_range
        }

    def extract_email(self, user_info: dict) -> dict:
        """
        Extracts an email address from the user's biography.
        """
        user_data = user_info.get('data', {}).get('user', {})
        biography = user_data.get('biography', '') or ''
        
        # Regex pattern for email extraction
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        
        # Find all matches in the biography
        emails = re.findall(email_pattern, biography)
        
        # Return the first found email, or None
        if emails:
            return {'email': emails[0]}
        else:
            return {'email': None}

    def extract_first_and_last_name(self, user_info: dict) -> dict:
        """
        Extracts the first and last name from the 'full_name' field.
        """
        user_data = user_info.get('data', {}).get('user', {})
        full_name = user_data.get('full_name', '') or ''
        
        names = full_name.split()
        first_name = names[0] if names else None
        last_name = " ".join(names[1:]) if len(names) > 1 else None
        
        return {'first_name': first_name, 'last_name': last_name}

    def determine_creator_size(self, user_info: dict) -> dict:
        """
        Determines the creator's size based on their follower count.
        """
        user_data = user_info.get('data', {}).get('user', {})
        follower_count = user_data.get('follower_count', 0)
        
        creator_size = None
        
        if follower_count:
            if follower_count < 5000:
                creator_size = "Nano-Influencer"
            elif follower_count < 50000:
                creator_size = "Micro-Influencer"
            elif follower_count < 500000:
                creator_size = "Mid-Tier Influencer"
            elif follower_count < 1000000:
                creator_size = "Macro-Influencer"
            else:
                creator_size = "Mega-Influencer"
        else:
            creator_size = "Unknown"
                
        return creator_size

    def extract_phone_number(self, user_info: dict) -> dict:
        """
        Extracts a phone number from the user's biography using multiple regular expressions.
        """
        user_data = user_info.get('data', {}).get('user', {})
        biography = user_data.get('biography', '') or ''
        
        patterns = [
            r'\+?\d{1,4}[-.\s]?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{4}',
            r'\+\d{10,15}',
            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\s*x\d{1,5}',
            r'\d{3,}[-.\s]?\d{3,}[-.\s]?\d{4,}'
        ]

        for pattern in patterns:
            match = re.search(pattern, biography)
            if match:
                phone_number = re.sub(r'[\s.-]', '', match.group(0))
                return {'phone_number': phone_number.strip()}
        
        return {'phone_number': None}

    def analyze_creator_data_with_social_links(self, creator_dir: str) -> dict:
        """Enhanced version of analyze_creator_data that includes hashtag, mention, social link, and gender extraction."""
        try:
            user_info_path = os.path.join(creator_dir, 'userInfo.json')
            post_info_path = os.path.join(creator_dir, 'postInfo.json')
            
            if not os.path.exists(user_info_path) or not os.path.exists(post_info_path):
                print(f"{Fore.RED}Missing required files in {creator_dir}{Style.RESET_ALL}")
                return None
            
            user_info = self.load_json_file(user_info_path)
            post_info = self.load_json_file(post_info_path)
            
            if not user_info or not post_info:
                print(f"{Fore.RED}Failed to load data for {creator_dir}{Style.RESET_ALL}")
                return None
            
            # Extract social media links
            social_links = self.extract_social_links(user_info)
            
            # Identify gender
            gender = self.identify_gender(user_info)

            email_info = self.extract_email(user_info)
            first_and_lastname = self.extract_first_and_last_name(user_info)
            creator_size = self.determine_creator_size(user_info)
            phone_number_info = self.extract_phone_number(user_info)

            # Calculate engagement rate metrics
            total_posts, top_posts, avg_er = self.calculate_top_post_er(post_info, user_info)
            
            # Get all posts for analysis
            all_posts = post_info.get('data', {}).get('xdt_api__v1__feed__user_timeline_graphql_connection', {}).get('edges', [])
            
            follower_count = user_info.get('data', {}).get('user', {}).get('follower_count', 0)
            engagement_metrics = self.calculate_engagement_metrics(post_info, follower_count)
            
            bio_text = user_info.get('data', {}).get('user', {}).get('biography', '')
            partnership_data = self.detect_paid_partnerships(post_info, bio_text)
            
            # Analyze location data
            location_data = self.analyze_location_data(post_info)
            
            # Detect fake followers
            user_data = user_info.get('data', {}).get('user', {})
            fake_follower_data = self.detect_fake_followers(user_data, post_info)
            
            # Analyze profile personality
            personality_traits, profile_insights = self.analyze_profile_personality(bio_text, post_info, engagement_metrics)
            
            # Identify collaborations (now includes UGC examples)
            collaboration_data = self.identify_collaborations(all_posts)
            
            # Identify niche
            niche_data = self.identify_niche(user_info)
            
            # Identify creator type
            creator_pricing_info = self.extract_creator_pricing(user_info, all_posts)
            
            # Extract hashtags and mentions from last 90 days
            hashtag_mention_data = self.extract_hashtags_and_mentions(all_posts, limit=10)

            basic_info = self.extract_basic_info(user_info)
            # Classify account type (personal / brand / creator)
            ig_account_type = self.classify_instagram_account(user_info)
            
            return {
                'username': basic_info.get('username'),
                'full_name': basic_info.get('full_name'),
                'ig_account_type': ig_account_type,
                'first_name' : first_and_lastname.get('first_name'),
                'last_name' : first_and_lastname.get('last_name'),
                'biography': basic_info.get('biography'),
                'phone_number' : phone_number_info.get('phone_number'),
                'follower_count': basic_info.get('follower_count'),
                'creator_size' : creator_size,
                'gender': gender,  
                'email' : email_info.get('email'),
                'business_category' : basic_info.get('category'),
                'profile_picture' : basic_info.get('profile_picture'),
                'social_links': social_links,
                'total_posts_last_3_months': total_posts,
                'top_6_posts': top_posts,
                'average_engagement_rate': avg_er,
                'collaboration_status': collaboration_data['status'],
                'total_collaborations': collaboration_data['total_collaborations'],
                'recent_collaborations': collaboration_data['recent_collaborations'],
                'ugc_examples': collaboration_data['ugc_examples'],
                'top_collaboration' : collaboration_data['all_collaborations'],
                'niche_data': niche_data,
                'creator_type': creator_pricing_info.get('creator_type'),
                'tier' : creator_pricing_info.get('tier'),
                'creator_pricing_metrics' : creator_pricing_info.get('creator_pricing_metrics'),
                'hashtags_last_90_days': hashtag_mention_data['hashtags'],
                'mentions_last_90_days': hashtag_mention_data['mentions'],
                'posts_analyzed_for_hashtags': hashtag_mention_data['total_posts_analyzed'],
                'hashtag_analysis_date_range': hashtag_mention_data['date_range'],
                'engagement_metrics': engagement_metrics,
                'partnership_data': partnership_data,
                'location_analysis': location_data,
                'fake_follower_analysis': fake_follower_data,
                'personality_traits': personality_traits,
                'profile_insights': profile_insights,
                'analyzed_date': datetime.datetime.now().strftime('%Y-%m-%d'),
                'scraped_date': datetime.datetime.now().strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            print(f"{Fore.RED}Error analyzing creator data in {creator_dir}: {str(e)}{Style.RESET_ALL}")
            return None

    def classify_instagram_account(self, user_info: dict) -> str:
        """
        Heuristically classify an Instagram profile as 'personal', 'brand', or 'creator'.
        Uses category, bio, external_url, and business flags.
        """
        try:
            user = user_info.get('data', {}).get('user', {}) if isinstance(user_info, dict) else {}
            category = (user.get('category') or user.get('business_category') or "") or ""
            bio = (user.get('biography') or "") or ""
            external = (user.get('external_url') or "") or ""
            is_business = user.get('is_business_account') or user.get('is_business') or False
            follower_count = user.get('follower_count') or 0

            c = (category or "").lower()
            b = (bio or "").lower()
            ext = (external or "").lower()

            # Strong brand signals
            brand_keywords = ['brand', 'store', 'shop', 'company', 'co.', 'official', 'boutique', 'ecommerce', 'retail']
            creator_keywords = ['creator', 'influencer', 'content', 'artist', 'photographer', 'videographer', 'ugc']

            if any(k in c for k in brand_keywords) or any(k in b for k in brand_keywords) or is_business:
                return 'brand'
            if any(k in c for k in creator_keywords) or any(k in b for k in creator_keywords):
                return 'creator'

            # external url often hints (shop links -> brand)
            if any(k in ext for k in ['shop', 'store', 'etsy', 'amazon', 'buy', 'products']):
                return 'brand'

            # verified accounts with professional follower count often creators/brands
            if user.get('is_verified'):
                if follower_count and follower_count > 50000:
                    return 'creator'

            # Default fallback -> personal
            return 'personal'
        except Exception:
            return 'unknown'
        
    def generate_txt_summary(self, analysis_data: dict, output_path: str) -> bool:
        """Generate a comprehensive text summary file for analyzed data."""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 120 + "\n")
                f.write("COMPREHENSIVE INSTAGRAM PROFILE ANALYSIS & INSIGHTS REPORT \n")
                f.write("=" * 120 + "\n\n")
                
                # Profile Overview Section
                f.write("📊 PROFILE OVERVIEW\n")
                f.write("-" * 60 + "\n")
                f.write(f"Username: @{analysis_data.get('username', 'N/A')}\n")
                f.write(f"Full Name: {analysis_data.get('full_name', 'N/A')}\n")
                follower = analysis_data.get('follower_count', 0) or 0
                try:
                    f.write(f"Follower Count: {int(follower):,}\n")
                except Exception:
                    f.write(f"Follower Count: {follower}\n")
                f.write(f"Creator Size: {analysis_data.get('creator_size', 'N/A')}\n")
                f.write(f"Gender: {analysis_data.get('gender', 'Unknown')}\n")
                f.write(f"Business Category: {analysis_data.get('business_category', 'N/A')}\n")
                f.write(f"Email: {analysis_data.get('email', 'N/A')}\n")
                f.write(f"Phone: {analysis_data.get('phone_number', 'N/A')}\n")
                f.write("\n")
                
                # Engagement Metrics
                f.write("📈 ENGAGEMENT METRICS\n")
                f.write("-" * 60 + "\n")
                f.write(f"Average Engagement Rate: {analysis_data.get('average_engagement_rate', 0)}%\n")
                f.write(f"Total Posts (Last 3 Months): {analysis_data.get('total_posts_last_3_months', 0)}\n")
                f.write(f"Collaboration Status: {analysis_data.get('collaboration_status', 'None')}\n")
                f.write(f"Total Collaborations: {analysis_data.get('total_collaborations', 0)}\n")
                f.write(f"Creator Type: {analysis_data.get('creator_type', 'N/A')}\n")
                f.write(f"Tier: {analysis_data.get('tier', 'N/A')}\n")
                f.write("\n")
                
                # Niche Analysis
                niche_data = analysis_data.get('niche_data', {})
                f.write("🎨 CONTENT ANALYSIS\n")
                f.write("-" * 60 + "\n")
                f.write(f"Primary Niche: {niche_data.get('overall_niche', 'N/A')}\n")
                if niche_data.get('distribution'):
                    f.write("Niche Distribution:\n")
                    for niche, percentage in niche_data['distribution'].items():
                        f.write(f"  • {niche}: {percentage}%\n")
                f.write("\n")
                
                # Top Hashtags
                hashtags = analysis_data.get('hashtags_last_90_days', {})
                if hashtags:
                    f.write("🏷️ TOP HASHTAGS (Last 90 Days)\n")
                    f.write("-" * 60 + "\n")
                    for hashtag, count in list(hashtags.items())[:10]:
                        f.write(f"#{hashtag}: {count} uses\n")
                    f.write("\n")
                
                # Top Mentions
                mentions = analysis_data.get('mentions_last_90_days', {})
                if mentions:
                    f.write("👥 TOP MENTIONS (Last 90 Days)\n")
                    f.write("-" * 60 + "\n")
                    for mention, count in list(mentions.items())[:10]:
                        f.write(f"@{mention}: {count} mentions\n")
                    f.write("\n")
                
                # Biography
                f.write("📖 BIOGRAPHY\n")
                f.write("-" * 60 + "\n")
                f.write(f"{analysis_data.get('biography', 'No biography available.')}\n\n")
                
                # Report Footer
                f.write("=" * 120 + "\n")
                f.write(f"📅 Report Generated: {analysis_data.get('analyzed_date', 'N/A')}\n")
                f.write("📱 Platform: Instagram\n")
                f.write("🤖 Analysis Version: Instagram Analytics OOP\n")
                f.write("=" * 120 + "\n")
            
            return True
        except Exception as e:
            print(f"{Fore.RED}Error generating text summary: {str(e)}{Style.RESET_ALL}")
            return False

    def run_analysis(self):
        """Main function to run the analysis on all creator folders."""
        print(f"{Fore.CYAN}Instagram Engagement Rate Calculator with Enhanced Analysis{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Analyzing profiles and generating comprehensive reports{Style.RESET_ALL}")
        print()
        
        # Get base path for creator folders
        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.output_dir)
        
        if not os.path.exists(base_path):
            print(f"{Fore.RED}Output directory not found: {base_path}{Style.RESET_ALL}")
            return
        
        creator_folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
        
        if not creator_folders:
            print(f"{Fore.RED}No creator folders found in {base_path}{Style.RESET_ALL}")
            return
        
        print(f"{Fore.GREEN}Found {len(creator_folders)} creator folders{Style.RESET_ALL}")
        print()
        
        # Store all results for summary and batch saving
        all_results = []
        successful_analyses = 0
        failed_analyses = 0
        
        # Process all creators
        for i, creator_folder in enumerate(creator_folders, 1):
            creator_path = os.path.join(base_path, creator_folder)
            
            print(f"{Fore.GREEN}[{i}/{len(creator_folders)}] Analyzing creator: {creator_folder}{Style.RESET_ALL}")
            
            result = self.analyze_creator_data_with_social_links(creator_path)
            
            if result:
                successful_analyses += 1
                all_results.append(result)
                
                # Generate individual text summary
                txt_path = os.path.join(creator_path, f"{creator_folder}_analysis.txt")
                self.generate_txt_summary(result, txt_path)
                
                print(f"  ✓ Username: {result['username']}")
                print(f"  ✓ Full Name: {result['full_name']}")
                print(f"  ✓ Gender: {result['gender']}")
                try:
                    print(f"  ✓ Follower Count: {int(result['follower_count']):,}")
                except Exception:
                    print(f"  ✓ Follower Count: {result['follower_count']}")
                print(f"  ✓ Average ER: {result['average_engagement_rate']}%")
                print(f"  ✓ Creator Type: {result['creator_type']}")
                print(f"  ✓ Generated text summary: {txt_path}")
                    
            else:
                failed_analyses += 1
                print(f"  {Fore.RED}✗ Failed to analyze{Style.RESET_ALL}")
            
            print()  # Add spacing between creators
        
        # Display summary
        print(f"{Fore.CYAN}=== ANALYSIS SUMMARY ==={Style.RESET_ALL}")
        print(f"Total creators found: {len(creator_folders)}")
        print(f"Successfully analyzed: {successful_analyses}")
        print(f"Failed analyses: {failed_analyses}")
        print()
        
        if all_results:
            # Sort results by engagement rate (highest first)
            sorted_results = sorted(all_results, key=lambda x: x.get('average_engagement_rate', 0), reverse=True)
            
            # Save enhanced results
            combined_results = {
                'analysis_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_creators_analyzed': successful_analyses,
                'creators': sorted_results
            }
            
            combined_output_file = "analyzed.json"
            try:
                with open(combined_output_file, 'w', encoding='utf-8') as f:
                    json.dump(combined_results, f, indent=2, ensure_ascii=False)
                
                print(f"\n{Fore.GREEN}Analysis results saved to: {combined_output_file}{Style.RESET_ALL}")
                
            except Exception as e:
                print(f"\n{Fore.RED}Error saving analysis results: {str(e)}{Style.RESET_ALL}")
        
        else:
            print(f"{Fore.RED}No creators were successfully analyzed.{Style.RESET_ALL}")

if __name__ == "__main__":
    analyzer = InstagramAnalyzer()
    analyzer.run_analysis()
