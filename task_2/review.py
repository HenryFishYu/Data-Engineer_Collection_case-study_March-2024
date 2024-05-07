import json
import re
from datetime import datetime, timezone

INAPPROPRIATE_WORDS_THRESHOLD = 0.2
INAPPROPRIATE_CHARACTERS_THRESHOLD = 0.5


class Review:
    def __init__(self, restaurant_id: int, review_id: int, text: str, rating: float, published_at: datetime):
        self.restaurant_id = restaurant_id
        self.review_id = review_id
        self.text = text
        self.rating = rating
        self.published_at = published_at

    @classmethod
    def from_json(cls, json_data):
        restaurant_id = json_data.get("restaurantId")
        review_id = json_data.get("reviewId")
        text = json_data.get("text")
        rating = json_data.get("rating")
        published_at_str = json_data.get("publishedAt")

        if None in (restaurant_id, review_id, text, rating, published_at_str):
            raise ValueError("All fields are required.")

        published_at = datetime.fromisoformat(published_at_str)
        return cls(restaurant_id, review_id, text, rating, published_at)

    def to_json_str(self):
        json_data = {
            "restaurantId": self.restaurant_id,
            "reviewId": self.review_id,
            "text": self.text,
            "rating": self.rating,
            "publishedAt": self.published_at.isoformat()
        }
        return json.dumps(json_data)

    def __repr__(self):
        return f"Review(restaurant_id={self.restaurant_id}, review_id={self.review_id}, text='{self.text}', rating={self.rating}, published_at={self.published_at})"

    def inappropriate_words_filter(self, inappropriate_words_set: set[str]) -> bool:
        number_of_illegal_words = 0
        words_list = self.text.split(" ")
        for i in range(len(words_list)):
            for inappropriate_word in inappropriate_words_set:
                english_words = re.findall(r'\b[a-zA-Z]+\b', words_list[i].upper())
                if english_words.__contains__(inappropriate_word) & (
                        len(inappropriate_word) / len(english_words) > INAPPROPRIATE_CHARACTERS_THRESHOLD):
                    number_of_illegal_words += 1
                    words_list[i] = re.sub(r'[a-zA-Z]', '*', words_list[i])
        self.text = " ".join(words_list)
        return number_of_illegal_words / len(words_list) < INAPPROPRIATE_WORDS_THRESHOLD


class RestaurantReviewAggregate:
    def __init__(self):
        self.restaurantId = None
        self.reviewCount = None
        self.totalRating = None
        self.totalReviewLength = None
        self.reviewAge = None
        self.inited = False

    def add_review(self, review: Review):
        number_of_days_until_today = (datetime.now(tz=timezone.utc) - review.published_at).days

        if self.inited:
            review_age = self.reviewAge
            review_age.oldest = max(number_of_days_until_today, review_age.oldest)
            review_age.newest = min(number_of_days_until_today, review_age.oldest)
            review_age.total += number_of_days_until_today
            self.reviewCount += 1
            self.totalRating += review.rating
            self.totalReviewLength += len(review.text.split(" "))
            return

        review_age = ReviewAge()
        review_age.oldest = number_of_days_until_today
        review_age.newest = number_of_days_until_today
        review_age.total = number_of_days_until_today
        self.restaurantId = review.restaurant_id
        self.reviewCount = 1
        self.totalRating = review.rating
        self.totalReviewLength = len(review.text.split(" "))
        self.reviewAge = review_age
        self.inited = True

    def to_json_str(self):
        result_review_dict = {
            "oldest": self.reviewAge.oldest,
            "newest": self.reviewAge.newest,
            "average": self.reviewAge.total / self.reviewCount
        }

        result_aggregation_dict = {
            "restaurantId": self.restaurantId, "reviewCount": self.reviewCount,
            "averageRating": self.totalRating / self.reviewCount,
            "averageReviewLength": self.totalReviewLength / self.reviewCount,
            "reviewAge": result_review_dict
        }

        return json.dumps(result_aggregation_dict)


class ReviewAge:
    def __init__(self):
        self.oldest = None
        self.newest = None
        self.total = None
