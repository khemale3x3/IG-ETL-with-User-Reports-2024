import json
import csv
import os
import datetime
from colorama import init, Fore, Style

# Initialize colorama for colored console output
init(autoreset=True)

def load_json_file(file_path: str) -> dict:
    """
    Load and parse a JSON file safely.
    
    Args:
        file_path (str): The path to the JSON file.
        
    Returns:
        dict: The parsed JSON data, or an empty dictionary if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"{Fore.RED}Error loading JSON file {file_path}: {str(e)}{Style.RESET_ALL}")
        return {}

def create_csv_from_analyzed_json(analyzed_json_path: str, output_csv_path: str):
    """
    Convert the analyzed.json file to a CSV format with each user as a separate row.
    This version flattens and includes all analyzed fields and also stores the raw JSON for each creator.
    
    Args:
        analyzed_json_path (str): The path to the JSON file.
        output_csv_path (str): The desired path for the output CSV file.
    
        Returns:
        tuple[bool, int]: A tuple indicating success status and the number of rows converted.
    """
    data = load_json_file(analyzed_json_path)
    if not data or not data.get('creators'):
        print(f"{Fore.RED}No data or creators found in analyzed.json file.{Style.RESET_ALL}")
        return False, 0

    creators = data.get('creators', [])
    creators.sort(key=lambda x: x.get('average_engagement_rate', 0), reverse=True)

    headers = [
        "username", "full_name", "first_name", "last_name", "biography",
        "age_group", "age", "gender",
        "email", "phone_number", "follower_count", "creator_size", "creator_type", "tier",
        "business_category", "profile_picture",
        # social links
        "tiktok_link", "youtube_link", "linktree_link", "other_social_media", "primary_social_link",
        # basic metrics
        "total_posts_last_3_months", "average_engagement_rate", "total_posts_analyzed", "engagement_rate_overall",
        "avg_likes", "avg_comments", "avg_shares", "avg_engagement_per_post",
        "consistency_score", "viral_posts_count", "post_frequency", "engagement_trend", "best_posting_time",
        "post1_interaction_score", "post1_likes", "post1_comments", "post1_er", "post1_code", "post1_url", "post1_date",
        "post2_interaction_score", "post2_likes", "post2_comments", "post2_er", "post2_code", "post2_url", "post2_date",
        "post3_interaction_score", "post3_likes", "post3_comments", "post3_er", "post3_code", "post3_url", "post3_date",
        "post4_interaction_score", "post4_likes", "post4_comments", "post4_er", "post4_code", "post4_url", "post4_date",
        "post5_interaction_score", "post5_likes", "post5_comments", "post5_er", "post5_code", "post5_url", "post5_date",
        "post6_interaction_score", "post6_likes", "post6_comments", "post6_er", "post6_code", "post6_url", "post6_date",
        # partnerships & collaborations
        "collaboration_status", "total_collaborations", "recent_collaborations", "ugc_examples",
        "has_paid_partnerships", "total_sponsored_posts", "avg_sponsored_engagement", "avg_organic_engagement", "collaboration_score",
        "sponsor_frequency", "brand_mentions",
        "location_tags", "most_visited_locations", "location_hashtags", "timezone_estimate", "location_diversity_score",
        "bio_location", "primary_location_lat", "primary_location_lng", "primary_location_names", "primary_location_post_count",
        "matched_city", "matched_state", "matched_country", "matched_timezone", "location_distance_km",
        # fake follower analysis
        "fake_follower_score", "authenticity_score", "engagement_quality", "suspicious_indicators",
        # personality & profile
        "personality_traits", "profile_insights",
        # niche
        "niche_overall", "niche_distribution", "niche_confidence_scores",
        # hashtags / mentions last 90 days
        "top_hashtags_90d", "top_mentions_90d", "posts_analyzed_for_hashtags", "hashtag_analysis_date_range",
        # creator pricing
        "creator_pricing_metrics", "estimated_roi", "impressions_visibility",
        "time_15_seconds", "time_30_seconds", "time_60_seconds", "time_1_to_5_minutes", "time_greater_than_5_minutes",
        # aggregated price and top collaborations
        "price_usd", "top_collaboration", "top_collaboration_brand_logo",
        # metadata
        "analyzed_date", "scraped_date", "source",
        # raw JSON
        "raw_analysis"
    ]

    all_rows = []
    for creator in creators:
        # Basic fields
        username = creator.get('username', '') or ''
        full_name = creator.get('full_name', '') or ''
        first_name = creator.get('first_name', '') or ''
        last_name = creator.get('last_name', '') or ''
        biography = (creator.get('biography') or '').replace('\n', ' ').strip()
        # demographic fields
        gender = creator.get('gender', '') or ''
        age = _compute_age_from_creator(creator)
        age_group = _age_group_from_age(age)
        email = creator.get('email', '') or ''
        phone_number = creator.get('phone_number', '') or ''
        follower_count = creator.get('follower_count', 0) or 0
        creator_size = creator.get('creator_size', '') or ''
        creator_type = creator.get('creator_type', '') or ''
        tier = creator.get('tier', '') or ''
        business_category = creator.get('business_category', '') or ''
        profile_picture = creator.get('profile_picture', '') or ''

        # Social links
        social_links = creator.get('social_links', {}) or {}
        tiktok_link = social_links.get('tiktok', '') or ''
        youtube_link = social_links.get('youtube', '') or ''
        linktree_link = social_links.get('linktree', '') or ''
        other_social_media = " | ".join([l for l in [tiktok_link, youtube_link, linktree_link] if l])
        primary_social_link = f"https://www.instagram.com/{username}" if username else ''

        # Engagement and metrics
        total_posts_last_3_months = creator.get('total_posts_last_3_months', 0)
        average_engagement_rate = creator.get('average_engagement_rate', 0)
        engagement_metrics = creator.get('engagement_metrics', {}) or {}
        total_posts_analyzed = engagement_metrics.get('total_posts_analyzed', '')
        engagement_rate_overall = engagement_metrics.get('engagement_rate', '')
        avg_likes = engagement_metrics.get('avg_likes', '')
        avg_comments = engagement_metrics.get('avg_comments', '')
        avg_shares = engagement_metrics.get('avg_shares', '')
        avg_engagement_per_post = engagement_metrics.get('avg_engagement_per_post', '')
        consistency_score = engagement_metrics.get('consistency_score', '')
        viral_posts_count = engagement_metrics.get('viral_posts_count', '')
        post_frequency = engagement_metrics.get('post_frequency', '')
        engagement_trend = engagement_metrics.get('engagement_trend', '')
        best_posting_time = engagement_metrics.get('best_posting_time', '')

        top_posts = creator.get('top_6_posts', []) or []
        top_post_cells = []
        for i in range(6):
            if i < len(top_posts):
                p = top_posts[i]
                top_post_cells.extend([
                    p.get('interaction_score', ''),
                    p.get('likes', p.get('like_count', '')),
                    p.get('comments', p.get('comment_count', '')),
                    p.get('engagement_rate', ''),
                    p.get('post_code', p.get('code', p.get('shortcode', ''))),
                    p.get('post_url', f"https://www.instagram.com/p/{p.get('shortcode', '')}/"),
                    p.get('taken_at', '')
                ])
            else:
                top_post_cells.extend(['', '', '', '', '', '', ''])

        # Partnerships and collaborations
        collaboration_status = creator.get('collaboration_status', '') or ''
        total_collaborations = creator.get('total_collaborations', 0)
        recent_collaborations = creator.get('recent_collaborations', 0)
        ugc_examples = creator.get('ugc_examples', '') or ''

        partnership_data = creator.get('partnership_data', {}) or {}
        has_paid_partnerships = partnership_data.get('has_paid_partnerships', '')
        total_sponsored_posts = partnership_data.get('total_sponsored_posts', '')
        avg_sponsored_engagement = partnership_data.get('avg_sponsored_engagement', '')
        avg_organic_engagement = partnership_data.get('avg_organic_engagement', '')
        collaboration_score = partnership_data.get('collaboration_score', '')
        sponsor_frequency = partnership_data.get('sponsor_frequency', {})
        brand_mentions = partnership_data.get('brand_mentions', [])

        location_analysis = creator.get('location_analysis', {}) or {}
        location_tags = " | ".join(location_analysis.get('location_tags', []))
        most_visited_locations = " | ".join(location_analysis.get('most_visited_locations', []))
        location_hashtags = " | ".join(location_analysis.get('location_hashtags', []))
        timezone_estimate = location_analysis.get('timezone_estimate', '')
        location_diversity_score = location_analysis.get('location_diversity_score', '')
        
        bio_location = location_analysis.get('bio_location', '')
        primary_location = location_analysis.get('primary_location', {})
        primary_location_lat = primary_location.get('lat', '') if primary_location else ''
        primary_location_lng = primary_location.get('lng', '') if primary_location else ''
        primary_location_names = " | ".join(primary_location.get('location_names', [])) if primary_location else ''
        primary_location_post_count = primary_location.get('post_count', '') if primary_location else ''
        
        # State/country data from matched city
        state_country_data = location_analysis.get('state_country_data', {})
        matched_city = state_country_data.get('city', '')
        matched_state = state_country_data.get('state_name', '')
        matched_country = state_country_data.get('country', '')
        matched_timezone = state_country_data.get('timezone', '')
        location_distance_km = state_country_data.get('distance_km', '')

        # Fake followers
        fake = creator.get('fake_follower_analysis', {}) or {}
        fake_follower_score = fake.get('fake_follower_score', '')
        authenticity_score = fake.get('authenticity_score', '')
        engagement_quality = fake.get('engagement_quality', '')
        suspicious_indicators = " | ".join(fake.get('suspicious_indicators', []) or [])

        # Personality & niche
        personality_traits = creator.get('personality_traits', {}) or {}
        profile_insights = creator.get('profile_insights', {}) or {}
        niche_data = creator.get('niche_data', {}) or {}
        niche_overall = niche_data.get('overall_niche', '')
        niche_distribution = json.dumps(niche_data.get('distribution', {}), ensure_ascii=False)
        niche_confidence_scores = json.dumps(niche_data.get('confidence_scores', {}), ensure_ascii=False)

        # Hashtags / mentions
        hashtags_last_90 = creator.get('hashtags_last_90_days', {}) or {}
        top_hashtags_90d = " | ".join([k for k, v in sorted(hashtags_last_90.items(), key=lambda x: x[1], reverse=True)[:10]])
        mentions_last_90 = creator.get('mentions_last_90_days', {}) or {}
        top_mentions_90d = " | ".join([k for k, v in sorted(mentions_last_90.items(), key=lambda x: x[1], reverse=True)[:10]])
        posts_analyzed_for_hashtags = creator.get('posts_analyzed_for_hashtags', '')
        hashtag_analysis_date_range = creator.get('hashtag_analysis_date_range', '')

        # Creator pricing
        creator_pricing = creator.get('creator_pricing_metrics', {}) or {}
        estimated_roi = creator_pricing.get('estimated_roi', creator.get('creator_pricing_metrics', {}).get('estimated_roi', ''))
        impressions_visibility = creator_pricing.get('impressions_visibility', '')
        time_15_seconds = creator_pricing.get('time_15_seconds', '')
        time_30_seconds = creator_pricing.get('time_30_seconds', '')
        time_60_seconds = creator_pricing.get('time_60_seconds', '')
        time_1_to_5_minutes = creator_pricing.get('time_1_to_5_minutes', '')
        time_greater_than_5_minutes = creator_pricing.get('time_greater_than_5_minutes', '')

        # Aggregated price and top collaborations
        price_usd_list = []
        creator_pricing_metrics = creator.get('creator_pricing_metrics', {})
        if creator_pricing_metrics:
            price_usd_list.append(f"TIME_15_SECONDS:{creator_pricing_metrics.get('time_15_seconds', '')}")
            price_usd_list.append(f"TIME_30_SECONDS:{creator_pricing_metrics.get('time_30_seconds', '')}")
            price_usd_list.append(f"TIME_60_SECONDS:{creator_pricing_metrics.get('time_60_seconds', '')}")
            price_usd_list.append(f"TIME_1_TO_5_MINUTES:{creator_pricing_metrics.get('time_1_to_5_minutes', '')}")
            price_usd_list.append(f"TIME_GREATER_THAN_5_MINUTES:{creator_pricing_metrics.get('time_greater_than_5_minutes', '')}")
        price_usd = '|'.join(price_usd_list)

        top_collaboration_list = [
            c.get('name') for c in creator.get('top_collaboration', [])
            if c.get('source') in ['paid_partnership', 'tag']
        ]
        top_collaboration_str = " | ".join(top_collaboration_list)

        top_collaboration_brand_logo_list = []
        for collab in creator.get('top_collaboration', []):
            if collab.get('source') in ['paid_partnership', 'tag']:
                brand_name = collab.get('name', '')
                if brand_name:
                    logo_url = f"https://assets.veelapp.com/{brand_name.replace(' ', '_').lower()}.jpg"
                    top_collaboration_brand_logo_list.append(f"{brand_name};{logo_url}")
        top_collaboration_brand_logo = " | ".join(top_collaboration_brand_logo_list)

        analyzed_date = creator.get('analyzed_date', '')
        scraped_date = creator.get('scraped_date', '')
        source = creator.get('source', '')

        # raw JSON as fallback for any missing data
        raw_analysis = json.dumps(creator, ensure_ascii=False)

        row = [
            username, full_name, first_name, last_name, biography,
            age_group, age, gender,
            email, phone_number, follower_count, creator_size, creator_type, tier,
            business_category, profile_picture,
            tiktok_link, youtube_link, linktree_link, other_social_media, primary_social_link,
            total_posts_last_3_months, average_engagement_rate, total_posts_analyzed, engagement_rate_overall,
            avg_likes, avg_comments, avg_shares, avg_engagement_per_post,
            consistency_score, viral_posts_count, post_frequency, engagement_trend, best_posting_time
        ]

        # add enhanced top posts with URLs
        row.extend(top_post_cells)

        # partnerships
        row.extend([
            collaboration_status, total_collaborations, recent_collaborations, ugc_examples,
            has_paid_partnerships, total_sponsored_posts, avg_sponsored_engagement, avg_organic_engagement, collaboration_score,
            json.dumps(sponsor_frequency, ensure_ascii=False), " | ".join(brand_mentions or [])
        ])

        row.extend([
            location_tags, most_visited_locations, location_hashtags, timezone_estimate, location_diversity_score,
            bio_location, primary_location_lat, primary_location_lng, primary_location_names, primary_location_post_count,
            matched_city, matched_state, matched_country, matched_timezone, location_distance_km
        ])

        # fake followers
        row.extend([
            fake_follower_score, authenticity_score, engagement_quality, suspicious_indicators
        ])

        # personality / niche
        row.extend([
            json.dumps(personality_traits, ensure_ascii=False), json.dumps(profile_insights, ensure_ascii=False),
            niche_overall, niche_distribution, niche_confidence_scores
        ])

        # hashtags / mentions
        row.extend([
            top_hashtags_90d, top_mentions_90d, posts_analyzed_for_hashtags, hashtag_analysis_date_range
        ])

        # pricing & metadata
        row.extend([
            json.dumps(creator_pricing, ensure_ascii=False), estimated_roi, impressions_visibility,
            time_15_seconds, time_30_seconds, time_60_seconds, time_1_to_5_minutes, time_greater_than_5_minutes,
            price_usd, top_collaboration_str, top_collaboration_brand_logo,
            analyzed_date, scraped_date, source
        ])

        # raw json
        row.append(raw_analysis)

        # Remove commas from simple strings to avoid CSV confusion
        cleaned_row = [str(item).replace(',', ' ') if isinstance(item, str) else item for item in row]
        all_rows.append(cleaned_row)

    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            writer.writerows(all_rows)
        return True, len(creators)
    except Exception as e:
        print(f"{Fore.RED}Error writing CSV file: {str(e)}{Style.RESET_ALL}")
        return False, 0

def display_social_media_stats(creators: list):
    """Display social media platform statistics."""
    if not creators:
        return
    
    tiktok_count = sum(1 for c in creators if c.get('social_links', {}).get('tiktok'))
    youtube_count = sum(1 for c in creators if c.get('social_links', {}).get('youtube'))
    linktree_count = sum(1 for c in creators if c.get('social_links', {}).get('linktree'))
    total_creators = len(creators)
    
    print(f"{Fore.YELLOW}=== SOCIAL MEDIA PLATFORM STATISTICS ==={Style.RESET_ALL}")
    print(f"Creators with TikTok: {tiktok_count}/{total_creators} ({tiktok_count/total_creators*100:.1f}%)")
    print(f"Creators with YouTube: {youtube_count}/{total_creators} ({youtube_count/total_creators*100:.1f}%)")
    print(f"Creators with Linktree: {linktree_count}/{total_creators} ({linktree_count/total_creators*100:.1f}%)")
    
    multi_platform_count = sum(1 for c in creators if sum(1 for link in c.get('social_links', {}).values() if link) > 1)
    print(f"Creators with multiple platforms: {multi_platform_count}/{total_creators} ({multi_platform_count/total_creators*100:.1f}%)")
    print()

def _compute_age_from_creator(creator: dict):
    """Try to determine numeric age from common fields: 'age', 'birth_year', 'dob', 'date_of_birth'."""
    today = datetime.date.today()
    # direct age field
    age = creator.get('age') or creator.get(' Age')  # tolerant lookup
    try:
        if age is not None and age != '':
            return int(age)
    except Exception:
        pass

    # birth_year
    birth_year = creator.get('birth_year') or creator.get('birthYear') or creator.get('year_of_birth')
    try:
        if birth_year:
            by = int(birth_year)
            return today.year - by
    except Exception:
        pass

    # dob / date_of_birth
    dob_candidates = [creator.get('dob'), creator.get('date_of_birth'), creator.get('dateOfBirth')]
    for dob in dob_candidates:
        if not dob:
            continue
        # try ISO and common formats
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                dt = datetime.datetime.strptime(dob, fmt).date()
                return int((today - dt).days // 365)
            except Exception:
                continue
        # try fromisoformat fallback
        try:
            dt = datetime.date.fromisoformat(dob)
            return int((today - dt).days // 365)
        except Exception:
            continue
    return ''

def _age_group_from_age(age):
    """Return age group string for a numeric age, or empty string."""
    try:
        age = int(age)
    except Exception:
        return ''
    if age < 18:
        return "Under 18"
    if 18 <= age <= 24:
        return "18-24"
    if 25 <= age <= 34:
        return "25-34"
    if 35 <= age <= 44:
        return "35-44"
    if 45 <= age <= 54:
        return "45-54"
    if 55 <= age <= 64:
        return "55-64"
    return "65+"

def main():
    """Main function to convert analyzed.json to CSV with today's date and social media links."""
    print(f"{Fore.CYAN}Enhanced Analyzed JSON to CSV Converter{Style.RESET_ALL}")
    print(f"{Fore.CYAN}This script converts 'analyzed.json' into a detailed CSV report.{Style.RESET_ALL}")
    print()
    
    analyzed_json_file = "analyzed.json"
    today_date = datetime.datetime.now().strftime('%Y%m%d')
    output_csv_file = f"output{today_date}.csv"
    
    if not os.path.exists(analyzed_json_file):
        print(f"{Fore.RED}analyzed.json file not found!{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Please ensure you have run the analysis script first.{Style.RESET_ALL}")
        return
    
    print(f"{Fore.GREEN}Found analyzed.json file.{Style.RESET_ALL}")
    print(f"{Fore.GREEN}Output will be saved as: {output_csv_file}{Style.RESET_ALL}")
    print()
    
    analyzed_data = load_json_file(analyzed_json_file)
    if analyzed_data:
        analysis_date = analyzed_data.get('analysis_date', analyzed_data.get('analysis_date', 'Unknown'))
        total_creators = analyzed_data.get('total_creators_analyzed', 0)
        print(f"{Fore.CYAN}Analysis Information:{Style.RESET_ALL}")
        print(f"  Analysis Date: {analysis_date}")
        print(f"  Total Creators: {total_creators}")
        print()
    
    if analyzed_data and analyzed_data.get('creators'):
        creators_count = len(analyzed_data['creators'])
        print(f"{Fore.CYAN}Found {creators_count} creators in analyzed.json.{Style.RESET_ALL}")
        display_social_media_stats(analyzed_data['creators'])
    
    print(f"{Fore.CYAN}Converting to CSV...{Style.RESET_ALL}")
    
    success, total_users = create_csv_from_analyzed_json(analyzed_json_file, output_csv_file)
    
    if success:
        print(f"{Fore.GREEN}✓ CSV file created successfully: {output_csv_file}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}✓ Total users converted: {total_users}{Style.RESET_ALL}")
        
        if analyzed_data and analyzed_data.get('creators'):
            print(f"\n{Fore.YELLOW}=== TOP 5 PERFORMERS PREVIEW ==={Style.RESET_ALL}")
            top_creators = analyzed_data['creators'][:5]
            for i, creator in enumerate(top_creators, 1):
                username = creator.get('username', 'N/A')
                full_name = creator.get('full_name', '')
                avg_er = creator.get('average_engagement_rate', 0)
                followers = creator.get('follower_count', 0)
                full_name_display = f" ({full_name})" if full_name else ""
                
                social_links = creator.get('social_links', {})
                platforms = []
                if social_links.get('tiktok'): platforms.append('TikTok')
                if social_links.get('youtube'): platforms.append('YouTube')
                if social_links.get('linktree'): platforms.append('Linktree')
                platforms_str = ', '.join(platforms) if platforms else 'Instagram only'
                
                print(f"  {i}. {username}{full_name_display} | {avg_er}% ER | {followers:,} followers")
                print(f"     Platforms: {platforms_str}")
        
        print(f"\n{Fore.GREEN}✓ Conversion completed successfully!{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}CSV columns include key metrics and the raw JSON for each creator.{Style.RESET_ALL}")
        
    else:
        print(f"{Fore.RED}✗ Conversion failed!{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
