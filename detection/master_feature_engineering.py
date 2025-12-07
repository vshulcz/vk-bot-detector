from collections import Counter, defaultdict
import re
import sqlite3
import pandas as pd
import numpy as np
import sys
from scipy import stats
import os
from pathlib import Path
from datetime import datetime, timezone

sys.path.append(str(Path(__file__).parent))


NOW_TS = int(datetime.now(timezone.utc).timestamp())


def calculate_text_entropy(text: str) -> float:
    if not text or len(text) < 2:
        return 0.0

    char_counts = Counter(text.lower())
    total = sum(char_counts.values())
    entropy = -sum(
        (count / total) * np.log2(count / total) for count in char_counts.values()
    )
    return entropy


def calculate_lexical_diversity(text: str) -> float:
    words = re.findall(r"\b\w+\b", text.lower())
    if len(words) < 2:
        return 0.0

    unique_words = len(set(words))
    total_words = len(words)
    return unique_words / total_words


def detect_caps_abuse(text: str) -> float:
    if not text:
        return 0.0

    letters = [c for c in text if c.isalpha()]
    if not letters:
        return 0.0

    return sum(c.isupper() for c in letters) / len(letters)


def count_emoji_density(text: str) -> float:
    if not text:
        return 0.0

    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"
        "\U0001f300-\U0001f5ff"
        "\U0001f680-\U0001f6ff"
        "\U0001f1e0-\U0001f1ff"
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "]+",
        flags=re.UNICODE,
    )

    emoji_count = len(emoji_pattern.findall(text))
    return emoji_count / len(text) if text else 0.0


def detect_url_patterns(text: str) -> dict[str, int]:
    if not text:
        return {"short_urls": 0, "suspicious_domains": 0, "total_urls": 0}

    url_pattern = re.compile(r"https?://[^\s]+|www\.[^\s]+")
    urls = url_pattern.findall(text)

    short_url_services = ["bit.ly", "goo.gl", "t.co", "tinyurl.com", "ow.ly"]
    short_urls = sum(
        1 for url in urls if any(service in url for service in short_url_services)
    )

    suspicious_tlds = [".xyz", ".top", ".click", ".loan", ".download", ".stream"]
    suspicious = sum(1 for url in urls if any(tld in url for tld in suspicious_tlds))

    return {
        "short_urls": short_urls,
        "suspicious_domains": suspicious,
        "total_urls": len(urls),
    }


def detect_spam_keywords(text: str) -> int:
    if not text:
        return 0

    text_lower = text.lower()

    spam_keywords = [
        "заработ",
        "дохoд",
        "бизнес",
        "вакансия",
        "работа на дому",
        "млн руб",
        "кредит",
        "займ",
        "casino",
        "казино",
        "ставки",
        "букмекер",
        "выиграл",
        "лотерея",
        "похудe",
        "диета",
        "таблетки",
        "препарат",
        "подписывайтесь",
        "переходи по ссылке",
        "жми сюда",
        "скидка",
        "акция",
        "бесплатно",
        "халява",
        "вот тут",
        "вот здесь",
        "тут можно",
        "здесь можно",
    ]

    return sum(1 for keyword in spam_keywords if keyword in text_lower)


def calculate_punctuation_ratio(text: str) -> float:
    if not text:
        return 0.0

    punct_count = sum(1 for c in text if c in ".,!?;:…")
    return punct_count / len(text)


def detect_repetitive_chars(text: str) -> int:
    if not text:
        return 0

    pattern = re.compile(r"(.)\1{2,}")
    return len(pattern.findall(text))


def calculate_avg_word_length(text: str) -> float:
    words = re.findall(r"\b\w+\b", text)
    if not words:
        return 0.0

    return sum(len(w) for w in words) / len(words)


def build_comment_text_features(conn) -> pd.DataFrame:
    comments = pd.read_sql(
        """
        SELECT 
            from_id as user_id,
            text,
            timestamp
        FROM comments
        WHERE from_id IS NOT NULL 
          AND from_id > 0
          AND text IS NOT NULL 
          AND text != ''
    """,
        conn,
    )

    if comments.empty:
        return pd.DataFrame({"user_id": []})

    text_features_list = []

    for user_id, group in comments.groupby("user_id"):
        if len(group) == 0:
            continue

        texts = group["text"].tolist()
        all_text = " ".join(texts)

        total_chars = sum(len(t) for t in texts)
        total_words = sum(len(re.findall(r"\b\w+\b", t)) for t in texts)

        features = {
            "user_id": user_id,
            "total_comments": len(texts),
            "total_chars": total_chars,
            "total_words": total_words,
            "avg_comment_length": total_chars / len(texts),
            "avg_words_per_comment": total_words / len(texts),
            "avg_text_entropy": np.mean([calculate_text_entropy(t) for t in texts]),
            "avg_lexical_diversity": np.mean(
                [calculate_lexical_diversity(t) for t in texts]
            ),
            "global_lexical_diversity": calculate_lexical_diversity(all_text),
            "caps_abuse_rate": np.mean([detect_caps_abuse(t) for t in texts]),
            "emoji_density": np.mean([count_emoji_density(t) for t in texts]),
            "avg_word_length": np.mean([calculate_avg_word_length(t) for t in texts]),
            "punctuation_ratio": np.mean(
                [calculate_punctuation_ratio(t) for t in texts]
            ),
            "repetitive_chars_avg": np.mean(
                [detect_repetitive_chars(t) for t in texts]
            ),
            "spam_keywords_total": sum(detect_spam_keywords(t) for t in texts),
            "spam_keywords_per_comment": sum(detect_spam_keywords(t) for t in texts)
            / len(texts),
            "short_urls_total": sum(
                detect_url_patterns(t)["short_urls"] for t in texts
            ),
            "suspicious_domains_total": sum(
                detect_url_patterns(t)["suspicious_domains"] for t in texts
            ),
            "urls_per_comment": sum(detect_url_patterns(t)["total_urls"] for t in texts)
            / len(texts),
            "unique_comments_ratio": len(set(texts)) / len(texts),
            "most_common_comment_freq": Counter(texts).most_common(1)[0][1]
            / len(texts),
            "cyrillic_char_ratio": sum(len(re.findall(r"[а-яёА-ЯЁ]", t)) for t in texts)
            / (total_chars or 1),
            "latin_char_ratio": sum(len(re.findall(r"[a-zA-Z]", t)) for t in texts)
            / (total_chars or 1),
            "question_mark_rate": sum(t.count("?") for t in texts) / len(texts),
            "exclamation_mark_rate": sum(t.count("!") for t in texts) / len(texts),
            "ellipsis_rate": sum(t.count("...") + t.count("…") for t in texts)
            / len(texts),
        }

        all_words = []
        for t in texts:
            words = re.findall(r"\b\w+\b", t.lower())
            all_words.extend(words)

        if len(all_words) >= 3:
            trigrams = [tuple(all_words[i : i + 3]) for i in range(len(all_words) - 2)]
            if trigrams:
                trigram_counts = Counter(trigrams)
                features["most_common_trigram_freq"] = trigram_counts.most_common(1)[0][
                    1
                ] / len(trigrams)
                features["unique_trigrams_ratio"] = len(set(trigrams)) / len(trigrams)
            else:
                features["most_common_trigram_freq"] = 0
                features["unique_trigrams_ratio"] = 0
        else:
            features["most_common_trigram_freq"] = 0
            features["unique_trigrams_ratio"] = 0

        if len(texts) >= 5:
            first_words = [t.split()[0].lower() if t.split() else "" for t in texts]
            first_word_counts = Counter(first_words)
            features["first_word_repetition"] = first_word_counts.most_common(1)[0][
                1
            ] / len(texts)
        else:
            features["first_word_repetition"] = 0

        text_features_list.append(features)

    result = pd.DataFrame(text_features_list)

    result["text_quality_score"] = (
        result["avg_text_entropy"] * 0.3
        + result["avg_lexical_diversity"] * 0.3
        + result["global_lexical_diversity"] * 0.4
    )

    result["spam_score"] = (
        result["spam_keywords_per_comment"] * 2
        + result["short_urls_total"] * 1.5
        + result["suspicious_domains_total"] * 3
        + result["caps_abuse_rate"] * 1
        + (1 - result["unique_comments_ratio"]) * 2
    )

    result["bot_text_likelihood"] = (
        (1 - result["text_quality_score"]) * 0.4
        + result["spam_score"] / 10 * 0.3
        + (1 - result["unique_comments_ratio"]) * 0.3
    )

    return result


def build_profile_text_features(conn) -> pd.DataFrame:
    profiles = pd.read_sql(
        """
        SELECT 
            id,
            user_id,
            first_name,
            last_name,
            status,
            about,
            interests,
            books,
            tv,
            quotes,
            games,
            movies,
            music
        FROM profiles
    """,
        conn,
    )

    features_list = []

    for _, row in profiles.iterrows():
        text_fields = [
            "status",
            "about",
            "interests",
            "books",
            "tv",
            "quotes",
            "games",
            "movies",
            "music",
        ]
        profile_texts = [
            str(row[f])
            for f in text_fields
            if pd.notna(row[f]) and str(row[f]) != "nan"
        ]

        if not profile_texts:
            features = {
                "user_id": row["user_id"],
                "profile_text_exists": 0,
                "profile_total_chars": 0,
                "profile_total_words": 0,
                "profile_entropy": 0,
                "profile_lexical_diversity": 0,
                "profile_spam_score": 0,
                "name_quality_score": 0,
            }
        else:
            all_profile_text = " ".join(profile_texts)
            total_chars = len(all_profile_text)
            total_words = len(re.findall(r"\b\w+\b", all_profile_text))

            features = {
                "user_id": row["user_id"],
                "profile_text_exists": 1,
                "profile_total_chars": total_chars,
                "profile_total_words": total_words,
                "profile_fields_filled": len(profile_texts),
                "profile_entropy": calculate_text_entropy(all_profile_text),
                "profile_lexical_diversity": calculate_lexical_diversity(
                    all_profile_text
                ),
                "profile_avg_field_length": total_chars / len(profile_texts),
                "profile_spam_keywords": sum(
                    detect_spam_keywords(t) for t in profile_texts
                ),
                "profile_urls_count": sum(
                    detect_url_patterns(t)["total_urls"] for t in profile_texts
                ),
            }

            features["profile_spam_score"] = (
                features["profile_spam_keywords"] * 2
                + features["profile_urls_count"] * 1.5
            ) / (len(profile_texts) + 1)

        first_name = str(row["first_name"]) if pd.notna(row["first_name"]) else ""
        last_name = str(row["last_name"]) if pd.notna(row["last_name"]) else ""

        name_features = {
            "first_name_len": len(first_name),
            "last_name_len": len(last_name),
            "name_has_numbers": int(bool(re.search(r"\d", first_name + last_name))),
            "name_has_special_chars": int(
                bool(re.search(r"[^а-яёА-ЯЁa-zA-Z\s-]", first_name + last_name))
            ),
            "name_caps_abuse": (
                detect_caps_abuse(first_name) + detect_caps_abuse(last_name)
            )
            / 2,
        }

        name_features["name_quality_score"] = 1.0
        if name_features["name_has_numbers"]:
            name_features["name_quality_score"] -= 0.5
        if name_features["name_has_special_chars"]:
            name_features["name_quality_score"] -= 0.3
        if name_features["name_caps_abuse"] > 0.5:
            name_features["name_quality_score"] -= 0.2
        if name_features["first_name_len"] < 2 or name_features["last_name_len"] < 2:
            name_features["name_quality_score"] -= 0.3

        name_features["name_quality_score"] = max(
            0, name_features["name_quality_score"]
        )

        features.update(name_features)
        features_list.append(features)

    return pd.DataFrame(features_list)


def build_linguistic_features(conn) -> pd.DataFrame:
    comments = pd.read_sql(
        """
        SELECT 
            from_id as user_id,
            text
        FROM comments
        WHERE from_id IS NOT NULL 
          AND from_id > 0
          AND text IS NOT NULL 
          AND text != ''
    """,
        conn,
    )

    if comments.empty:
        return pd.DataFrame({"user_id": []})

    features_list = []

    for user_id, group in comments.groupby("user_id"):
        texts = group["text"].tolist()

        sentences = []
        for t in texts:
            sent_list = re.split(r"[.!?]+", t)
            sentences.extend([s.strip() for s in sent_list if s.strip()])

        if not sentences:
            continue

        sentence_lengths = [len(re.findall(r"\b\w+\b", s)) for s in sentences]
        avg_sentence_length = np.mean(sentence_lengths) if sentence_lengths else 0

        sentence_length_std = (
            np.std(sentence_lengths) if len(sentence_lengths) > 1 else 0
        )

        all_words = []
        for t in texts:
            words = re.findall(r"\b\w+\b", t.lower())
            all_words.extend(words)

        vocab_size = len(set(all_words))
        total_words = len(all_words)

        window_size = 50
        if total_words >= window_size:
            mattr_values = []
            for i in range(total_words - window_size + 1):
                window = all_words[i : i + window_size]
                ttr = len(set(window)) / len(window)
                mattr_values.append(ttr)
            mattr = np.mean(mattr_values)
        else:
            mattr = vocab_size / total_words if total_words > 0 else 0

        word_freq = Counter(all_words)

        hapax_count = sum(1 for count in word_freq.values() if count == 1)
        hapax_ratio = hapax_count / total_words if total_words > 0 else 0

        function_words_ru = {
            "и",
            "в",
            "на",
            "с",
            "к",
            "о",
            "по",
            "для",
            "от",
            "из",
            "у",
            "за",
            "под",
            "над",
            "при",
            "через",
            "между",
            "что",
            "который",
            "как",
            "когда",
            "где",
            "почему",
            "если",
            "то",
            "но",
            "а",
            "или",
            "же",
            "ли",
            "бы",
        }

        function_word_count = sum(word_freq.get(w, 0) for w in function_words_ru)
        function_word_ratio = (
            function_word_count / total_words if total_words > 0 else 0
        )

        features = {
            "user_id": user_id,
            "avg_sentence_length": avg_sentence_length,
            "sentence_length_std": sentence_length_std,
            "vocab_size": vocab_size,
            "mattr": mattr,
            "hapax_ratio": hapax_ratio,
            "function_word_ratio": function_word_ratio,
            "avg_word_frequency": total_words / vocab_size if vocab_size > 0 else 0,
        }

        features["linguistic_sophistication"] = (
            (avg_sentence_length / 20) * 0.2
            + mattr * 0.3
            + hapax_ratio * 0.3
            + function_word_ratio * 0.2
        )

        features_list.append(features)

    return pd.DataFrame(features_list)


def build_all_text_features(sqlite_path: str, output_path: str):
    conn = sqlite3.connect(sqlite_path)
    comment_text = build_comment_text_features(conn)
    profile_text = build_profile_text_features(conn)
    linguistic = build_linguistic_features(conn)
    conn.close()

    result = profile_text.copy()
    if not comment_text.empty:
        result = result.merge(comment_text, on="user_id", how="left")

    if not linguistic.empty:
        result = result.merge(linguistic, on="user_id", how="left")

    for col in result.columns:
        if col != "user_id":
            if result[col].dtype in ["float64", "float32"]:
                result[col] = result[col].fillna(0)
            elif result[col].dtype in ["int64", "int32"]:
                result[col] = result[col].fillna(0)

    result.to_csv(output_path, index=False)

    return result


def build_follower_network_features(conn) -> pd.DataFrame:
    followers = pd.read_sql(
        """
        SELECT 
            profile_id,
            follower_user_id
        FROM profile_followers_sample
    """,
        conn,
    )

    if followers.empty:
        return pd.DataFrame({"id": []})

    follower_graph = defaultdict(set)
    following_graph = defaultdict(set)

    for _, row in followers.iterrows():
        profile_id = row["profile_id"]
        follower_id = row["follower_user_id"]

        follower_graph[profile_id].add(follower_id)
        following_graph[follower_id].add(profile_id)

    features_list = []

    for profile_id in follower_graph.keys():
        my_followers = follower_graph[profile_id]

        if not my_followers:
            continue

        mutual_connections_count = 0
        for f1 in my_followers:
            for f2 in my_followers:
                if f1 != f2 and f2 in follower_graph.get(f1, set()):
                    mutual_connections_count += 1

        max_possible_connections = len(my_followers) * (len(my_followers) - 1)
        follower_network_density = (
            mutual_connections_count / max_possible_connections
            if max_possible_connections > 0
            else 0
        )

        follower_subscriptions = []
        for follower in my_followers:
            subs = following_graph.get(follower, set())
            follower_subscriptions.append(subs)

        if len(follower_subscriptions) >= 2:
            similarities = []
            for i in range(len(follower_subscriptions)):
                for j in range(i + 1, len(follower_subscriptions)):
                    s1, s2 = follower_subscriptions[i], follower_subscriptions[j]
                    if len(s1) > 0 or len(s2) > 0:
                        intersection = len(s1 & s2)
                        union = len(s1 | s2)
                        sim = intersection / union if union > 0 else 0
                        similarities.append(sim)

            avg_follower_similarity = np.mean(similarities) if similarities else 0
        else:
            avg_follower_similarity = 0

        follower_out_degrees = [
            len(following_graph.get(f, set())) for f in my_followers
        ]

        features = {
            "id": profile_id,
            "follower_sample_size": len(my_followers),
            "follower_network_density": follower_network_density,
            "avg_follower_similarity": avg_follower_similarity,
            "follower_avg_out_degree": np.mean(follower_out_degrees)
            if follower_out_degrees
            else 0,
            "follower_median_out_degree": np.median(follower_out_degrees)
            if follower_out_degrees
            else 0,
            "follower_zero_out_degree_rate": sum(
                1 for d in follower_out_degrees if d == 0
            )
            / len(follower_out_degrees)
            if follower_out_degrees
            else 0,
        }

        features["followers_suspicious_cluster"] = int(
            avg_follower_similarity > 0.7 or follower_network_density > 0.5
        )

        features_list.append(features)

    return pd.DataFrame(features_list)


def build_subscription_network_features(conn) -> pd.DataFrame:
    subs = pd.read_sql(
        """
        SELECT 
            profile_id,
            group_id,
            type,
            screen_name
        FROM profile_subscriptions_sample
    """,
        conn,
    )

    if subs.empty:
        return pd.DataFrame({"id": []})

    profile_groups = defaultdict(set)
    group_subscribers = defaultdict(set)

    for _, row in subs.iterrows():
        profile_id = row["profile_id"]
        group_id = row["group_id"]

        profile_groups[profile_id].add(group_id)
        group_subscribers[group_id].add(profile_id)

    features_list = []

    for profile_id, groups in profile_groups.items():
        if not groups:
            continue

        group_popularity = [len(group_subscribers[g]) for g in groups]

        overlap_counts = []
        for group in groups:
            other_users = group_subscribers[group] - {profile_id}
            overlap_counts.append(len(other_users))

        group_types = subs[subs["profile_id"] == profile_id]["type"].value_counts()
        type_entropy = (
            -sum(
                (count / len(groups)) * np.log2(count / len(groups))
                for count in group_types.values
            )
            if len(group_types) > 1
            else 0
        )

        features = {
            "id": profile_id,
            "subscription_count": len(groups),
            "avg_group_popularity": np.mean(group_popularity)
            if group_popularity
            else 0,
            "median_group_popularity": np.median(group_popularity)
            if group_popularity
            else 0,
            "group_type_entropy": type_entropy,
            "avg_group_overlap": np.mean(overlap_counts) if overlap_counts else 0,
        }

        features["extreme_group_popularity"] = (
            int(np.mean(group_popularity) < 10 or np.mean(group_popularity) > 100000)
            if group_popularity
            else 0
        )

        features_list.append(features)

    return pd.DataFrame(features_list)


def build_comment_interaction_features(conn) -> pd.DataFrame:
    comments = pd.read_sql(
        """
        SELECT 
            from_id as user_id,
            owner_id,
            post_id,
            reply_to_comment_id
        FROM comments
        WHERE from_id IS NOT NULL AND from_id > 0
    """,
        conn,
    )

    if comments.empty:
        return pd.DataFrame({"user_id": []})

    replies = pd.read_sql(
        """
        SELECT 
            c1.from_id as replier_id,
            c2.from_id as replied_to_id
        FROM comments c1
        JOIN comments c2 ON c1.reply_to_comment_id = c2.comment_id 
                        AND c1.owner_id = c2.owner_id 
                        AND c1.post_id = c2.post_id
        WHERE c1.from_id IS NOT NULL 
          AND c2.from_id IS NOT NULL
          AND c1.from_id > 0
          AND c2.from_id > 0
    """,
        conn,
    )

    reply_network = defaultdict(list)
    replied_by = defaultdict(list)

    for _, row in replies.iterrows():
        replier = row["replier_id"]
        replied_to = row["replied_to_id"]

        reply_network[replier].append(replied_to)
        replied_by[replied_to].append(replier)

    user_posts = (
        comments.groupby("user_id")
        .agg({"owner_id": "nunique", "post_id": "count"})
        .reset_index()
    )
    user_posts.columns = ["user_id", "unique_post_owners", "total_comments"]

    features_list = []

    for user_id, group in comments.groupby("user_id"):
        replied_to_users = reply_network.get(user_id, [])
        replies_received = replied_by.get(user_id, [])

        unique_replied_to = len(set(replied_to_users))
        unique_replies_from = len(set(replies_received))

        replied_to_set = set(replied_to_users)
        replies_from_set = set(replies_received)
        mutual_interactions = len(replied_to_set & replies_from_set)

        post_owners = group["owner_id"].nunique()
        total_comments = len(group)

        features = {
            "user_id": user_id,
            "reply_out_degree": len(replied_to_users),
            "reply_in_degree": len(replies_received),
            "unique_interaction_partners": unique_replied_to,
            "unique_repliers": unique_replies_from,
            "reply_reciprocity_rate": mutual_interactions
            / max(unique_replied_to, unique_replies_from)
            if max(unique_replied_to, unique_replies_from) > 0
            else 0,
            "post_owner_diversity": post_owners,
            "comments_per_post_owner": total_comments / post_owners
            if post_owners > 0
            else 0,
        }

        features["focused_commenting"] = int(
            features["comments_per_post_owner"] > 10 or post_owners < 3
        )

        features["low_engagement"] = int(
            len(replies_received) < len(replied_to_users) * 0.1
        )

        features_list.append(features)

    return pd.DataFrame(features_list)


def calculate_local_clustering_coefficient(conn) -> pd.DataFrame:
    followers = pd.read_sql(
        """
        SELECT DISTINCT
            profile_id,
            follower_user_id
        FROM profile_followers_sample
    """,
        conn,
    )

    if followers.empty:
        return pd.DataFrame({"id": []})

    graph = defaultdict(set)
    for _, row in followers.iterrows():
        profile = row["profile_id"]
        follower = row["follower_user_id"]
        graph[follower].add(profile)

    features_list = []

    for node in graph.keys():
        neighbors = graph[node]

        if len(neighbors) < 2:
            clustering_coef = 0
        else:
            connected_pairs = 0
            for n1 in neighbors:
                for n2 in neighbors:
                    if n1 != n2 and n2 in graph.get(n1, set()):
                        connected_pairs += 1

            max_possible = len(neighbors) * (len(neighbors) - 1)
            clustering_coef = connected_pairs / max_possible if max_possible > 0 else 0

        features_list.append(
            {
                "user_id": node,
                "local_clustering_coefficient": clustering_coef,
                "neighbor_count": len(neighbors),
            }
        )

    result = pd.DataFrame(features_list)
    result["suspicious_low_clustering"] = (
        result["local_clustering_coefficient"] < 0.1
    ).astype(int)

    return result


def build_all_network_features(sqlite_path: str, output_path: str):
    conn = sqlite3.connect(sqlite_path)
    follower_net = build_follower_network_features(conn)
    sub_net = build_subscription_network_features(conn)
    comment_net = build_comment_interaction_features(conn)
    clustering = calculate_local_clustering_coefficient(conn)
    conn.close()

    all_users = pd.read_sql(
        "SELECT DISTINCT id, user_id FROM profiles", sqlite3.connect(sqlite_path)
    )

    result = all_users.copy()
    if not follower_net.empty:
        result = result.merge(follower_net, on="id", how="left")

    if not sub_net.empty:
        result = result.merge(sub_net, on="id", how="left")

    if not comment_net.empty:
        result = result.merge(comment_net, on="user_id", how="left")

    if not clustering.empty:
        result = result.merge(clustering, on="user_id", how="left")

    for col in result.columns:
        if col not in ["id", "user_id"]:
            if result[col].dtype in ["float64", "float32"]:
                result[col] = result[col].fillna(0)
            elif result[col].dtype in ["int64", "int32"]:
                result[col] = result[col].fillna(0)

    result.to_csv(output_path, index=False)
    return result


def build_temporal_features(conn) -> pd.DataFrame:
    profiles = pd.read_sql(
        """
        SELECT 
            p.id,
            p.user_id,
            p.registered_at,
            p.last_seen_ts,
            p.collected_at,
            p.online,
            c.followers,
            c.friends,
            c.photos,
            c.videos,
            c.audios,
            c.groups
        FROM profiles p
        LEFT JOIN profile_counters c ON p.id = c.profile_id
    """,
        conn,
    )

    profiles["registered_date"] = pd.to_datetime(
        profiles["registered_at"], errors="coerce"
    )
    profiles["account_age_days"] = (
        datetime.now() - profiles["registered_date"]
    ).dt.days

    profiles["total_activity"] = (
        pd.to_numeric(profiles["followers"], errors='coerce').fillna(0).astype('Int64')
        + pd.to_numeric(profiles["friends"], errors='coerce').fillna(0).astype('Int64')
        + pd.to_numeric(profiles["photos"], errors='coerce').fillna(0).astype('Int64')
        + pd.to_numeric(profiles["videos"], errors='coerce').fillna(0).astype('Int64')
        + pd.to_numeric(profiles["audios"], errors='coerce').fillna(0).astype('Int64')
        + pd.to_numeric(profiles["groups"], errors='coerce').fillna(0).astype('Int64')
    )

    profiles["activity_per_day"] = np.where(
        profiles["account_age_days"] > 0,
        profiles["total_activity"] / profiles["account_age_days"],
        0,
    )

    profiles["reg_year"] = profiles["registered_date"].dt.year
    profiles["reg_month"] = profiles["registered_date"].dt.month
    profiles["reg_day_of_week"] = profiles["registered_date"].dt.dayofweek
    profiles["reg_hour"] = profiles["registered_date"].dt.hour

    profiles["registered_night_hours"] = (
        (profiles["reg_hour"] >= 2) & (profiles["reg_hour"] <= 6)
    ).astype(int)

    profiles["days_since_last_seen"] = np.where(
        profiles["last_seen_ts"].notna(),
        (NOW_TS - profiles["last_seen_ts"]) / 86400.0,
        np.nan,
    )

    profiles["last_seen_to_age_ratio"] = np.where(
        (profiles["account_age_days"] > 0) & (profiles["days_since_last_seen"].notna()),
        profiles["days_since_last_seen"] / profiles["account_age_days"],
        np.nan,
    )

    reg_date_counts = profiles.groupby(profiles["registered_date"].dt.date).size()
    profiles["accounts_registered_same_day"] = (
        profiles["registered_date"].dt.date.map(reg_date_counts).fillna(0)
    )

    temporal_cols = [
        "id",
        "account_age_days",
        "activity_per_day",
        "reg_year",
        "reg_month",
        "reg_day_of_week",
        "reg_hour",
        "registered_night_hours",
        "days_since_last_seen",
        "last_seen_to_age_ratio",
        "accounts_registered_same_day",
    ]

    return profiles[temporal_cols]


def build_authenticity_features(conn) -> pd.DataFrame:
    profiles = pd.read_sql(
        """
        SELECT 
            p.id,
            p.first_name,
            p.last_name,
            p.nickname,
            p.bdate,
            p.city_title,
            p.country_title,
            p.home_town,
            p.status,
            p.about,
            p.interests,
            p.books,
            p.tv,
            p.quotes,
            p.games,
            p.movies,
            p.music,
            p.site,
            p.mobile_phone,
            p.home_phone,
            p.photo_50,
            p.photo_100,
            p.photo_200,
            p.photo_400,
            p.verified,
            p.is_sber_verified,
            p.is_tinkoff_verified,
            p.is_esia_verified
        FROM profiles p
    """,
        conn,
    )

    def check_cyrillic_ratio(text):
        if pd.isna(text) or str(text).strip() == "":
            return np.nan
        text = str(text)
        cyrillic = len(re.findall(r"[а-яёА-ЯЁ]", text))
        total = len(re.findall(r"[а-яёА-ЯЁa-zA-Z]", text))
        return cyrillic / total if total > 0 else 0

    profiles["first_name_cyrillic_ratio"] = profiles["first_name"].apply(
        check_cyrillic_ratio
    )
    profiles["last_name_cyrillic_ratio"] = profiles["last_name"].apply(
        check_cyrillic_ratio
    )

    profiles["name_mixed_alphabet"] = (
        (profiles["first_name_cyrillic_ratio"] > 0)
        & (profiles["first_name_cyrillic_ratio"] < 1)
    ).astype(int)

    profiles["first_name_len"] = profiles["first_name"].astype(str).str.len()
    profiles["last_name_len"] = profiles["last_name"].astype(str).str.len()
    profiles["full_name_len"] = profiles["first_name_len"] + profiles["last_name_len"]

    profiles["name_has_digits"] = (
        profiles["first_name"].astype(str).str.contains(r"\d", regex=True, na=False)
        | profiles["last_name"].astype(str).str.contains(r"\d", regex=True, na=False)
    ).astype(int)

    text_fields = [
        "status",
        "about",
        "interests",
        "books",
        "tv",
        "quotes",
        "games",
        "movies",
        "music",
    ]

    profiles["text_fields_filled_count"] = sum(
        profiles[field].notna() & (profiles[field].astype(str).str.strip() != "")
        for field in text_fields
    )

    def count_unique_words(row):
        text = " ".join([str(row[f]) for f in text_fields if pd.notna(row[f])])
        words = re.findall(r"\w+", text.lower())
        return len(set(words)) if words else 0

    profiles["profile_unique_words"] = profiles.apply(count_unique_words, axis=1)

    def avg_text_length(row):
        lengths = [
            len(str(row[f]))
            for f in text_fields
            if pd.notna(row[f]) and str(row[f]).strip() != ""
        ]
        return np.mean(lengths) if lengths else 0

    profiles["profile_avg_text_length"] = profiles.apply(avg_text_length, axis=1)

    photo_cols = ["photo_50", "photo_100", "photo_200", "photo_400"]
    profiles["photo_sizes_count"] = sum(
        profiles[col].notna() & (profiles[col].astype(str) != "") for col in photo_cols
    )

    profiles["has_contact_info"] = (
        profiles["site"].notna()
        | profiles["mobile_phone"].notna()
        | profiles["home_phone"].notna()
    ).astype(int)

    profiles["verification_count"] = (
        profiles["verified"].fillna(0).astype(bool).astype(int)
        + profiles["is_sber_verified"].fillna(0).astype(bool).astype(int)
        + profiles["is_tinkoff_verified"].fillna(0).astype(bool).astype(int)
        + profiles["is_esia_verified"].fillna(0).astype(bool).astype(int)
    )

    profiles["has_city"] = profiles["city_title"].notna().astype(int)
    profiles["has_country"] = profiles["country_title"].notna().astype(int)
    profiles["has_home_town"] = profiles["home_town"].notna().astype(int)
    profiles["location_consistency"] = (
        profiles["has_city"] + profiles["has_country"] + profiles["has_home_town"]
    )

    auth_cols = [
        "id",
        "first_name_cyrillic_ratio",
        "last_name_cyrillic_ratio",
        "name_mixed_alphabet",
        "first_name_len",
        "last_name_len",
        "full_name_len",
        "name_has_digits",
        "text_fields_filled_count",
        "profile_unique_words",
        "profile_avg_text_length",
        "photo_sizes_count",
        "has_contact_info",
        "verification_count",
        "location_consistency",
    ]

    return profiles[auth_cols]


def build_social_graph_features(conn) -> pd.DataFrame:
    counters = pd.read_sql(
        """
        SELECT 
            profile_id as id,
            followers,
            friends,
            groups,
            subscriptions,
            mutual_friends,
            online_friends
        FROM profile_counters
    """,
        conn,
    )

    counters["friends"] = pd.to_numeric(counters["friends"], errors='coerce').fillna(0)
    counters["followers"] = pd.to_numeric(counters["followers"], errors='coerce').fillna(0)

    counters["ff_ratio"] = np.where(
        counters["followers"] > 0, counters["friends"] / counters["followers"], np.inf
    )

    counters["ff_ratio_extreme"] = (
        (counters["ff_ratio"] > 10) | (counters["ff_ratio"] < 0.1)
    ).astype(int)

    counters["mutual_friends_rate"] = np.where(
        counters["friends"] > 0,
        pd.to_numeric(counters["mutual_friends"], errors='coerce').fillna(0) / counters["friends"],
        0,
    )

    counters["online_friends_rate"] = np.where(
        counters["friends"] > 0,
        pd.to_numeric(counters["online_friends"], errors='coerce').fillna(0) / counters["friends"],
        0,
    )

    counters["groups_to_subs_ratio"] = np.where(
        pd.to_numeric(counters["subscriptions"], errors='coerce').fillna(0) > 0,
        pd.to_numeric(counters["groups"], errors='coerce').fillna(0) / pd.to_numeric(counters["subscriptions"], errors='coerce').fillna(1),
        0,
    )

    followers_sample = pd.read_sql(
        """
        SELECT 
            profile_id,
            COUNT(*) as sample_size,
            AVG(CASE WHEN sex = 1 THEN 1 ELSE 0 END) as female_rate,
            AVG(CASE WHEN sex = 2 THEN 1 ELSE 0 END) as male_rate,
            AVG(CASE WHEN online = 1 THEN 1 ELSE 0 END) as online_rate,
            AVG(CASE WHEN photo_50 IS NOT NULL AND photo_50 != '' THEN 1 ELSE 0 END) as has_photo_rate
        FROM profile_followers_sample
        GROUP BY profile_id
    """,
        conn,
    )

    followers_sample["id"] = followers_sample["profile_id"]
    followers_sample["follower_gender_balance"] = 1 - abs(
        followers_sample["female_rate"] - followers_sample["male_rate"]
    )

    followers_sample["followers_suspicious_uniformity"] = (
        (followers_sample["has_photo_rate"] < 0.1)
        | (followers_sample["has_photo_rate"] > 0.95)
        | (followers_sample["online_rate"] > 0.8)
    ).astype(int)

    subs_sample = pd.read_sql(
        """
        SELECT 
            profile_id,
            COUNT(*) as subs_sample_size,
            COUNT(DISTINCT type) as subs_type_diversity,
            AVG(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as subs_has_desc_rate
        FROM profile_subscriptions_sample
        GROUP BY profile_id
    """,
        conn,
    )

    subs_sample["id"] = subs_sample["profile_id"]

    result = counters.merge(
        followers_sample[
            [
                "id",
                "sample_size",
                "follower_gender_balance",
                "followers_suspicious_uniformity",
                "online_rate",
                "has_photo_rate",
            ]
        ],
        on="id",
        how="left",
        suffixes=("", "_follower"),
    ).merge(
        subs_sample[
            ["id", "subs_sample_size", "subs_type_diversity", "subs_has_desc_rate"]
        ],
        on="id",
        how="left",
    )

    return result


def build_activity_pattern_features(conn) -> pd.DataFrame:
    comments = pd.read_sql(
        """
        SELECT 
            from_id as user_id,
            timestamp,
            text,
            likes,
            reply_to_comment_id,
            is_deleted,
            is_edited
        FROM comments
        WHERE from_id IS NOT NULL AND from_id > 0
    """,
        conn,
    )

    if comments.empty:
        return pd.DataFrame({"user_id": []})

    comments["datetime"] = pd.to_datetime(
        comments["timestamp"], unit="s", errors="coerce"
    )
    comments["hour"] = comments["datetime"].dt.hour
    comments["day_of_week"] = comments["datetime"].dt.dayofweek
    comments["text_len"] = comments["text"].astype(str).str.len()

    activity_patterns = []

    for user_id, group in comments.groupby("user_id"):
        if len(group) < 2:
            continue

        group = group.sort_values("timestamp")

        intervals = group["timestamp"].diff().dropna() / 60.0

        features = {
            "user_id": user_id,
            "comments_count": len(group),
            "comment_interval_mean_minutes": intervals.mean()
            if len(intervals) > 0
            else np.nan,
            "comment_interval_std_minutes": intervals.std()
            if len(intervals) > 0
            else np.nan,
            "comment_interval_cv": intervals.std() / intervals.mean()
            if len(intervals) > 0 and intervals.mean() > 0
            else np.nan,
            "suspiciously_regular_intervals": int(
                (intervals.std() / intervals.mean() < 0.5)
                if len(intervals) > 0 and intervals.mean() > 0
                else False
            ),
            "hour_entropy": stats.entropy(group["hour"].value_counts(normalize=True))
            if len(group) > 5
            else np.nan,
            "night_activity_rate": (group["hour"].between(0, 6).sum() / len(group)),
            "weekend_activity_rate": (
                group["day_of_week"].isin([5, 6]).sum() / len(group)
            ),
            "hour_distribution_uniformity": 1
            - (group["hour"].value_counts().std() / group["hour"].value_counts().mean())
            if len(group["hour"].value_counts()) > 1
            else 0,
            "text_len_mean": group["text_len"].mean(),
            "text_len_std": group["text_len"].std(),
            "text_len_cv": group["text_len"].std() / group["text_len"].mean()
            if group["text_len"].mean() > 0
            else np.nan,
            "unique_text_ratio": group["text"].nunique() / len(group)
            if len(group) > 0
            else 0,
            "most_common_text_freq": group["text"].value_counts().iloc[0] / len(group)
            if len(group) > 0
            else 0,
            "avg_likes_per_comment": group["likes"].mean(),
            "median_likes_per_comment": group["likes"].median(),
            "zero_likes_rate": (group["likes"] == 0).sum() / len(group),
            "reply_rate": group["reply_to_comment_id"].notna().sum() / len(group),
            "deleted_rate": pd.to_numeric(group["is_deleted"], errors='coerce').fillna(0).astype(int).sum() / len(group),
            "edited_rate": pd.to_numeric(group["is_edited"], errors='coerce').fillna(0).astype(int).sum() / len(group),
            "max_comments_in_hour": group.groupby(group["datetime"].dt.floor("h"))
            .size()
            .max(),
            "max_comments_in_day": group.groupby(group["datetime"].dt.date)
            .size()
            .max(),
            "activity_span_days": (group["timestamp"].max() - group["timestamp"].min())
            / 86400.0,
            "comments_per_active_day": len(group)
            / ((group["timestamp"].max() - group["timestamp"].min()) / 86400.0 + 1),
        }

        activity_patterns.append(features)

    activity_df = pd.DataFrame(activity_patterns)

    comment_urls = pd.read_sql(
        """
        SELECT c.from_id as user_id, COUNT(*) as url_count
        FROM comments c
        JOIN comment_urls cu ON c.id = cu.comment_id_fk
        WHERE c.from_id IS NOT NULL
        GROUP BY c.from_id
    """,
        conn,
    )

    comment_hashtags = pd.read_sql(
        """
        SELECT c.from_id as user_id, COUNT(*) as hashtag_count
        FROM comments c
        JOIN comment_hashtags ch ON c.id = ch.comment_id_fk
        WHERE c.from_id IS NOT NULL
        GROUP BY c.from_id
    """,
        conn,
    )

    comment_mentions = pd.read_sql(
        """
        SELECT c.from_id as user_id, COUNT(*) as mention_count
        FROM comments c
        JOIN comment_mentions cm ON c.id = cm.comment_id_fk
        WHERE c.from_id IS NOT NULL
        GROUP BY c.from_id
    """,
        conn,
    )

    result = activity_df.merge(comment_urls, on="user_id", how="left")
    result = result.merge(comment_hashtags, on="user_id", how="left")
    result = result.merge(comment_mentions, on="user_id", how="left")

    result["url_count"] = result["url_count"].fillna(0)
    result["hashtag_count"] = result["hashtag_count"].fillna(0)
    result["mention_count"] = result["mention_count"].fillna(0)

    result["url_per_comment"] = result["url_count"] / result["comments_count"]
    result["hashtag_per_comment"] = result["hashtag_count"] / result["comments_count"]
    result["mention_per_comment"] = result["mention_count"] / result["comments_count"]

    result["spam_score"] = (
        result["url_per_comment"] + result["hashtag_per_comment"] * 0.5
    )

    return result


def build_education_career_features(conn) -> pd.DataFrame:
    unis = pd.read_sql(
        """
        SELECT 
            profile_id,
            COUNT(*) as uni_count,
            MIN(graduation) as first_grad_year,
            MAX(graduation) as last_grad_year,
            SUM(CASE WHEN faculty_name IS NOT NULL THEN 1 ELSE 0 END) as has_faculty_count,
            SUM(CASE WHEN chair_name IS NOT NULL THEN 1 ELSE 0 END) as has_chair_count
        FROM profile_universities
        WHERE university_id IS NOT NULL OR name IS NOT NULL
        GROUP BY profile_id
    """,
        conn,
    )

    unis["id"] = unis["profile_id"]
    unis["uni_grad_span_years"] = unis["last_grad_year"] - unis["first_grad_year"]
    unis["uni_details_completeness"] = (
        unis["has_faculty_count"] + unis["has_chair_count"]
    ) / (unis["uni_count"] * 2)

    schools = pd.read_sql(
        """
        SELECT 
            profile_id,
            COUNT(*) as school_count,
            MIN(year_graduated) as first_school_grad,
            MAX(year_graduated) as last_school_grad,
            SUM(CASE WHEN speciality IS NOT NULL THEN 1 ELSE 0 END) as has_speciality_count
        FROM profile_schools
        WHERE school_id IS NOT NULL OR name IS NOT NULL
        GROUP BY profile_id
    """,
        conn,
    )

    schools["id"] = schools["profile_id"]
    schools["school_grad_span_years"] = (
        schools["last_school_grad"] - schools["first_school_grad"]
    )

    careers = pd.read_sql(
        """
        SELECT 
            profile_id,
            COUNT(*) as career_count,
            SUM(CASE WHEN position IS NOT NULL AND position != '' THEN 1 ELSE 0 END) as has_position_count,
            SUM(CASE WHEN company IS NOT NULL AND company != '' THEN 1 ELSE 0 END) as has_company_count,
            SUM(CASE WHEN until_year IS NOT NULL AND from_year IS NOT NULL 
                THEN until_year - from_year ELSE 0 END) as total_career_years
        FROM profile_careers
        GROUP BY profile_id
    """,
        conn,
    )

    careers["id"] = careers["profile_id"]
    careers["career_details_completeness"] = (
        careers["has_position_count"] + careers["has_company_count"]
    ) / (careers["career_count"] * 2)

    result = (
        unis[["id", "uni_count", "uni_grad_span_years", "uni_details_completeness"]]
        .merge(
            schools[["id", "school_count", "school_grad_span_years"]],
            on="id",
            how="outer",
        )
        .merge(
            careers[
                [
                    "id",
                    "career_count",
                    "total_career_years",
                    "career_details_completeness",
                ]
            ],
            on="id",
            how="outer",
        )
    )

    result = result.fillna(0)

    result["education_career_score"] = (
        (result["uni_count"] > 0).astype(int) * 3
        + (result["school_count"] > 0).astype(int) * 2
        + (result["career_count"] > 0).astype(int) * 2
        + result["uni_details_completeness"]
        + result["career_details_completeness"]
    )

    return result


def build_all_advanced_features(sqlite_path: str, output_path: str):
    conn = sqlite3.connect(sqlite_path)
    profiles = pd.read_sql("SELECT id, user_id, is_bot FROM profiles", conn)

    temporal = build_temporal_features(conn)
    authenticity = build_authenticity_features(conn)
    social = build_social_graph_features(conn)
    activity = build_activity_pattern_features(conn)
    education = build_education_career_features(conn)

    conn.close()

    result = profiles.merge(temporal, on="id", how="left")
    result = result.merge(authenticity, on="id", how="left")

    social_renamed = (
        social.rename(columns={"id": "profile_id"})
        if "id" in social.columns
        else social
    )
    if "profile_id" in social_renamed.columns:
        result = result.merge(
            social_renamed.drop(columns=["profile_id"], errors="ignore"),
            left_on="id",
            right_index=True,
            how="left",
        )

    if not activity.empty:
        result = result.merge(
            activity.drop(columns=["user_id"], errors="ignore"),
            left_on="user_id",
            right_on=activity["user_id"] if "user_id" in activity.columns else None,
            how="left",
        )

    result = result.merge(education, on="id", how="left")

    result["target_is_bot"] = result["is_bot"].fillna(-1).astype(int)
    result = result.drop(columns=["is_bot"], errors="ignore")

    for col in result.columns:
        if col in ["id", "user_id", "target_is_bot"]:
            continue

        if result[col].dtype in ["float64", "float32"]:
            if "ratio" in col or "rate" in col or "score" in col:
                result[col] = result[col].fillna(0)
            else:
                result[col] = result[col].fillna(result[col].median())
        elif result[col].dtype in ["int64", "int32"]:
            result[col] = result[col].fillna(0)

    result.to_csv(output_path, index=False)
    return result


def build_complete_feature_set(
    sqlite_path: str,
    output_dir: str = "features_output",
    final_output: str = "complete_features.csv",
):
    os.makedirs(output_dir, exist_ok=True)
    profile_features_path = os.path.join(output_dir, "profile_features.csv")

    try:
        profile_features = build_all_advanced_features(
            sqlite_path, profile_features_path
        )
    except Exception:
        import traceback

        traceback.print_exc()
        profile_features = None

    text_features_path = os.path.join(output_dir, "text_features.csv")
    try:
        text_features = build_all_text_features(sqlite_path, text_features_path)
    except Exception:
        import traceback

        traceback.print_exc()
        text_features = None

    network_features_path = os.path.join(output_dir, "network_features.csv")
    try:
        network_features = build_all_network_features(
            sqlite_path, network_features_path
        )
    except Exception:
        import traceback

        traceback.print_exc()
        network_features = None

    if profile_features is None:
        return None

    merged = profile_features.copy()
    if text_features is not None and not text_features.empty:
        merged = merged.merge(
            text_features.drop(columns=["id"], errors="ignore"),
            on="user_id",
            how="left",
            suffixes=("", "_text"),
        )

    if network_features is not None and not network_features.empty:
        merge_cols = []
        if "user_id" in network_features.columns:
            merge_cols.append("user_id")
        if "id" in network_features.columns and "id" in merged.columns:
            if "user_id" not in merge_cols:
                merge_cols.append("id")

        if merge_cols:
            cols_to_drop = [
                c
                for c in network_features.columns
                if c in merged.columns and c not in merge_cols
            ]
            network_to_merge = network_features.drop(
                columns=cols_to_drop, errors="ignore"
            )

            merged = merged.merge(
                network_to_merge, on=merge_cols, how="left", suffixes=("", "_net")
            )

    merged = merged.loc[:, ~merged.columns.duplicated()]
    for col in merged.select_dtypes(include=[np.number]).columns:
        merged[col] = merged[col].replace([np.inf, -np.inf], np.nan)

    for col in merged.columns:
        if col in ["id", "user_id", "target_is_bot"]:
            continue

        if merged[col].dtype in ["float64", "float32", "int64", "int32"]:
            if any(
                keyword in col.lower() for keyword in ["ratio", "rate", "score", "per_"]
            ):
                merged[col] = merged[col].fillna(0)
            elif any(
                keyword in col.lower() for keyword in ["count", "total", "sum", "num_"]
            ):
                merged[col] = merged[col].fillna(0)
            else:
                median_val = merged[col].median()
                merged[col] = merged[col].fillna(
                    median_val if pd.notna(median_val) else 0
                )

    final_path = os.path.join(output_dir, final_output)
    merged.to_csv(final_path, index=False)

    return merged


def main():
    sqlite_path = "vk.sqlite"
    output_dir = "features_output"
    final_output = "complete_features.csv"

    if not os.path.exists(sqlite_path):
        sys.exit(1)

    try:
        result = build_complete_feature_set(sqlite_path, output_dir, final_output)
        if result is None:
            sys.exit(1)

    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
