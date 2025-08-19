import re
import unicodedata
from difflib import SequenceMatcher


def normalize_award_id(award_id):
    if not award_id:
        return ""

    award_id_ascii = unicodedata.normalize('NFKD', str(award_id))
    award_id_ascii = award_id_ascii.encode('ascii', 'ignore').decode('ascii')

    normalized = re.sub(r'[^A-Za-z0-9]', '', award_id_ascii).upper()
    return normalized


def extract_segments(award_id):
    if not award_id:
        return []

    award_id_clean = str(award_id)
    for char in ['‐', '–', '—', '−']:
        award_id_clean = award_id_clean.replace(char, '-')

    award_id_ascii = unicodedata.normalize('NFKD', award_id_clean)
    award_id_ascii = award_id_ascii.encode('ascii', 'ignore').decode('ascii')

    segments = re.split(r'[-_./\s]+', award_id_ascii.strip())

    return [seg.upper() for seg in segments if seg]


def are_segments_compatible(seg1, seg2):
    if seg1 == seg2:
        return True

    if seg1.isdigit() and seg2.isdigit():
        if int(seg1) == int(seg2):
            return True

        if seg1.lstrip('0') == seg2.lstrip('0'):
            return True

        if len(seg1) == 4 and len(seg2) == 2:
            if seg1.endswith(seg2):
                return True
        elif len(seg2) == 4 and len(seg1) == 2:
            if seg2.endswith(seg1):
                return True

        return False

    if seg1.isdigit() != seg2.isdigit():
        return False

    if seg1.startswith(seg2) or seg2.startswith(seg1):
        seg1_nums = re.findall(r'\d+', seg1)
        seg2_nums = re.findall(r'\d+', seg2)
        if seg1_nums and seg2_nums:
            try:
                if all(int(n1) == int(n2) for n1, n2 in zip(seg1_nums, seg2_nums)):
                    return True
            except:
                pass

    seg1_alpha = re.sub(r'\d', '', seg1)
    seg2_alpha = re.sub(r'\d', '', seg2)

    if seg1_alpha == seg2_alpha:
        seg1_nums = re.findall(r'\d+', seg1)
        seg2_nums = re.findall(r'\d+', seg2)
        if seg1_nums and seg2_nums:
            if seg1_nums[0] != seg2_nums[0]:
                try:
                    if int(seg1_nums[0]) != int(seg2_nums[0]):
                        return False
                except:
                    return False
        return True

    if seg1 in seg2 or seg2 in seg1:
        return True

    return False


def structured_match(id1, id2):
    segments1 = extract_segments(id1)
    segments2 = extract_segments(id2)

    if not segments1 or not segments2:
        return False, 0.0

    # For very different lengths, likely not a match
    if abs(len(segments1) - len(segments2)) > 2:
        return False, 0.0

    matched_segments = 0
    total_segments = max(len(segments1), len(segments2))

    # Try to align segments
    for i in range(min(len(segments1), len(segments2))):
        if are_segments_compatible(segments1[i], segments2[i]):
            matched_segments += 1
        else:
            # Check if this is a critical segment (usually numeric identifiers)
            if segments1[i].isdigit() and segments2[i].isdigit():
                # Treat different numbers in the same position as different grants
                # with an exception for fir it's a year difference and other segments match
                if i == 0 or i == len(segments1) - 1 or i == len(segments2) - 1:
                    # First or last segment difference with numbers = likely different grant
                    return False, matched_segments / total_segments

    confidence = matched_segments / total_segments
    is_match = confidence >= 0.75

    return is_match, confidence


def levenshtein_distance(s1, s2):
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and current_row are one character longer than s2
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def calculate_similarity_ratio(s1, s2):
    if not s1 or not s2:
        return 0.0

    return SequenceMatcher(None, s1, s2).ratio()


def longest_common_substring_length(s1, s2):
    if not s1 or not s2:
        return 0

    m = len(s1)
    n = len(s2)

    # Table to store lengths of longest common suffixes
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    result = 0

    for i in range(m + 1):
        for j in range(n + 1):
            if i == 0 or j == 0:
                dp[i][j] = 0
            elif s1[i-1] == s2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
                result = max(result, dp[i][j])
            else:
                dp[i][j] = 0

    return result


def calculate_overlap_coefficient(s1, s2):
    if not s1 or not s2:
        return 0.0

    set1 = set(s1)
    set2 = set(s2)

    if len(set1) == 0 or len(set2) == 0:
        return 0.0

    intersection = len(set1 & set2)
    smaller_set_size = min(len(set1), len(set2))

    return intersection / smaller_set_size


def is_fuzzy_match(s1, s2, threshold: float = 0.90):
    if not s1 or not s2:
        return False

    if s1 == s2:
        return True

    s1_norm = normalize_award_id(s1)
    s2_norm = normalize_award_id(s2)

    # Exact match
    if s1_norm == s2_norm:
        return True

    # Substring match
    if s1_norm in s2_norm or s2_norm in s1_norm:
        return True

    # For structured IDs, use segment-based comparison
    is_structured_match, confidence = structured_match(s1, s2)
    if is_structured_match:
        return True

    # If structured matching fails with low confidence, skip other methods
    if confidence < 0.5:
        return False

    # Only proceed with fuzzy matching if the IDs don't look highly structured
    segments1 = extract_segments(s1)
    segments2 = extract_segments(s2)

    numeric_segments1 = sum(1 for seg in segments1 if seg.isdigit())
    numeric_segments2 = sum(1 for seg in segments2 if seg.isdigit())

    # If both have multiple numeric segments, they're likely structured IDs
    # so skip fuzzy matching
    if numeric_segments1 >= 2 and numeric_segments2 >= 2:
        return False

    len_s1 = len(s1_norm)
    len_s2 = len(s2_norm)

    if min(len_s1, len_s2) <= 3:
        return s1_norm == s2_norm

    similarity_ratio = calculate_similarity_ratio(s1_norm, s2_norm)

    if any(c.isdigit() for c in s1_norm) and any(c.isdigit() for c in s2_norm):
        return similarity_ratio >= 0.95

    return similarity_ratio >= threshold


def check_substring_match(id1, id2):
    if not id1 or not id2:
        return False

    id1_str = str(id1).strip()
    id2_str = str(id2).strip()

    if id1_str in id2_str or id2_str in id1_str:
        return True

    id1_norm = normalize_award_id(id1_str)
    id2_norm = normalize_award_id(id2_str)

    return id1_norm in id2_norm or id2_norm in id1_norm


def check_normalized_match(id1, id2):
    if not id1 or not id2:
        return False

    return normalize_award_id(id1) == normalize_award_id(id2)


def match_award_ids(id1, id2, match_types = None):
    if match_types is None:
        match_types = ['substring', 'normalized', 'fuzzy']

    if id1 is None or id2 is None:
        if id1 is None and id2 is None:
            return True, 'exact'
        return False, None

    id1_str = str(id1).strip()
    id2_str = str(id2).strip()

    if id1_str == id2_str:
        return True, 'exact'

    if 'substring' in match_types and check_substring_match(id1_str, id2_str):
        return True, 'substring'

    if 'normalized' in match_types and check_normalized_match(id1_str, id2_str):
        return True, 'normalized'

    if 'fuzzy' in match_types and is_fuzzy_match(id1_str, id2_str):
        return True, 'fuzzy'

    return False, None


def get_similarity_score(id1, id2):
    if id1 is None or id2 is None:
        if id1 is None and id2 is None:
            return 1.0
        return 0.0

    id1_str = str(id1).strip()
    id2_str = str(id2).strip()

    if id1_str == id2_str:
        return 1.0

    id1_norm = normalize_award_id(id1_str)
    id2_norm = normalize_award_id(id2_str)

    if id1_norm == id2_norm:
        return 0.95

    is_structured_match, structured_confidence = structured_match(
        id1_str, id2_str)

    segments1 = extract_segments(id1_str)
    segments2 = extract_segments(id2_str)
    numeric_segments1 = sum(1 for seg in segments1 if seg.isdigit())
    numeric_segments2 = sum(1 for seg in segments2 if seg.isdigit())

    if numeric_segments1 >= 2 and numeric_segments2 >= 2:
        return structured_confidence

    scores = []

    if id1_norm in id2_norm or id2_norm in id1_norm:
        containment_score = min(len(id1_norm), len(
            id2_norm)) / max(len(id1_norm), len(id2_norm))
        scores.append(max(0.9, containment_score))

    seq_similarity = calculate_similarity_ratio(id1_norm, id2_norm)
    scores.append(seq_similarity)

    if id1_norm and id2_norm:
        edit_dist = levenshtein_distance(id1_norm, id2_norm)
        max_len = max(len(id1_norm), len(id2_norm))
        edit_score = 1.0 - (edit_dist / max_len)
        scores.append(edit_score)

    if id1_norm and id2_norm:
        lcs_len = longest_common_substring_length(id1_norm, id2_norm)
        avg_len = (len(id1_norm) + len(id2_norm)) / 2
        lcs_score = lcs_len / avg_len
        scores.append(lcs_score)

    max_score = max(scores) if scores else 0.0

    if structured_confidence > 0:
        max_score = min(max_score, structured_confidence + 0.1)

    return max_score


def get_match_type(id1, id2, match_types = None):
    _, match_type = match_award_ids(id1, id2, match_types=match_types)
    return match_type


def awards_match(id1, id2, match_types = None):
    match_found, _ = match_award_ids(id1, id2, match_types=match_types)
    return match_found