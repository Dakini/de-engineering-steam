from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import Dict, List, Optional, Union
from datetime import datetime
import re


class GameDetails(BaseModel):
    appid: int = Field(..., description="Steam Application ID")
    name: str = Field(..., description="game's name")
    date_added: datetime = Field(..., description="Date of the scrape")
    developer: str = Field(
        ..., description="comma separated list of the developers of the game"
    )
    publisher: str = Field(
        ..., description="comma separated list of the publishers of the game"
    )
    score_rank: str = Field(
        "", description="score rank of the game based on user reviews"
    )
    positive: int = Field(..., description="number of positive reviews ")
    negative: int = Field(..., description="number of negative reviews ")
    userscore: float = (Field(..., description="User score of the game"),)
    owners: str = Field(
        ..., description="owners of this application on Steam as a range."
    )
    average_forever: int = Field(
        ..., description="average playtime since March 2009. In minutes"
    )
    average_2weeks: int = Field(
        ..., description="average playtime in the last two weeks. In minutes."
    )
    median_forever: int = Field(
        ..., description="median playtime since March 2009. In minutes"
    )
    median_2weeks: int = Field(
        ..., description="median playtime in the last two weeks. In minutes"
    )
    price: Optional[int] = Field(..., description="current US price in cents")
    initialprice: Optional[int] = Field(..., description="original US price in cents.")
    discount: Optional[str] = Field(..., description="current discount in percents.")
    ccu: int = Field("", description="peak CCU yesterday")
    languages: Optional[str] = Field(None, description="list of supported languages.")
    genre: Optional[str] = Field(None, description="list of genres.")
    tags: Optional[dict[str, int]] = Field(
        None, description="game's tags with votes in JSON array."
    )

    @field_validator("tags", mode="before")
    def validate_tags(cls, v):
        if v == []:
            return None
        if v is not None and not isinstance(v, dict):
            raise ValueError("Tags must be a dictionary or None")
        return v

    @field_validator("score_rank", mode="before")
    def validate_score_rank(cls, v):
        if isinstance(v, int):
            return int(v)

        if v is not None and not isinstance(v, str):
            raise ValueError("Score must be a string or integer")
        return v


class GameDetailsList(BaseModel):
    games: List[GameDetails] = Field(..., description="list of games")


def get_utc_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")


class SteamGameRank(BaseModel):
    appid: int = Field(..., description="Steam Application ID")
    rank: int = Field(..., description="game's current rank")
    last_week_rank: int = Field(..., description="game's rank last week")
    peak_in_game: int = Field(
        ..., description="peak number of players in the last 24 hours's"
    )
    date_added: datetime = Field(..., description="Date of the scrape")


class GameTop100Rank(BaseModel):
    ranks: List[SteamGameRank] = Field(..., description="list of games metadata")

    @classmethod
    def from_raw_data(cls, raw_data):
        """Parse raw data for pydantic model"""
        for data in raw_data:
            date_added = get_utc_timestamp(data["rollup_date"])
            ranks = [
                SteamGameRank(**game, date_added=date_added) for game in data["ranks"]
            ]
            break
        return cls(ranks=ranks)


class SteamGameMetadata(BaseModel):
    type: str = Field(..., description="game's type")
    name: str = Field(..., description="game's name")
    appid: int = Field(..., description="Steam Application ID")
    date_added: datetime = Field(..., description="date scraped")
    required_age: Optional[Union[int, str]] = Field(
        ..., description="Required age to play the game"
    )
    is_free: bool = Field(..., description="is the game free")
    dlc: Optional[list[int]] = Field(
        ..., description="list of dlc id's associated with the game"
    )
    controller_support: Optional[str] = Field(
        ..., description="Type of controller support for the game, if available"
    )
    about_the_game: Optional[str] = Field(..., description="About the game")
    detailed_description: Optional[str] = Field(..., description="game's description")
    short_description: Optional[str] = Field(
        ..., description="Short description of the game"
    )
    supported_languages: Optional[str] = Field(
        ..., description="Languages the game supports"
    )
    reviews: Optional[str] = Field(
        ..., description="Reviews or acclaim summary of the game"
    )
    header_image: str = Field(..., description="Url to the header image of the game ")
    capsule_image: str = Field(..., description="Url to the thumbnail of the game")
    website: Optional[str] = Field(..., description="The games website ")
    requirements: Optional[Dict] = Field(
        ..., description="PC system requirements for the game"
    )
    developers: Optional[List[str]] = Field(
        default=[], description="List of developers"
    )
    publishers: Optional[List[str]] = Field(..., description="List of publishers ")
    price_overview: Optional[Dict] = Field(
        ..., description="Price overview of the game"
    )
    platform: Optional[dict] = Field(
        ..., description="Indicates the platforms it is available on "
    )
    metacritic: Optional[int] = Field(
        ..., description="Metacritic score of the game if there"
    )
    categories: Optional[list] = Field(default=[], description="Categories of the game")
    genres: Optional[list] = Field(default=[], description="Genres the game belongs to")
    recommendations: int = Field(..., description="Recommendations numbers")
    achievements_number: int = Field(
        ..., description="Total number of attainable achievements"
    )
    release_date: Optional[str] = Field(
        ..., description="Date for when the game will release "
    )
    coming_soon: bool = Field(
        ..., description="Indicates if the game release is upcoming"
    )

    @field_validator("header_image", mode="before")
    def validate_header_image(cls, v):
        if isinstance(v, HttpUrl):
            v = str(v)
        elif not isinstance(v, str):
            raise ValueError(f"Invalid value for required age {v}")
        return v

    @field_validator("capsule_image", mode="before")
    def validate_capsule_image(cls, v):
        if isinstance(v, HttpUrl):
            v = str(v)
        elif not isinstance(v, str):
            raise ValueError(f"Invalid value for required age {v}")
        return v

    @field_validator("website", mode="before")
    def validate_website(cls, v):
        if v is None:
            return v
        if isinstance(v, (HttpUrl, str)):  # Single value
            return str(v)
        if isinstance(v, list):  # List of URLs
            new_v = []
            for vv in v:
                if isinstance(vv, HttpUrl):
                    vv = str(vv)
                elif not isinstance(vv, str):
                    raise ValueError(f"Invalid value for website: {vv}")
                new_v.append(vv)
            return new_v
        raise ValueError(f"Invalid value for website: {v}")

    @field_validator("required_age", mode="before")
    def validate_required_age(cls, v):
        if isinstance(v, str):
            match = re.search(r"\d+", v)
            if match:
                v = int(match.group())
            else:
                raise ValueError(f"Invalid value for required age {v}")
        elif not isinstance(v, int):
            raise ValueError(f"Invalid value for required age {v}")
        return v

    @field_validator("requirements", mode="before")
    def validate_requirements(cls, v):
        if isinstance(v, list):
            return {}
        else:
            return v


class SteamGameMetadataList(BaseModel):
    games: List[SteamGameMetadata] = Field(..., description="list of games")
