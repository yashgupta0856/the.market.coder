from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime

from web.db.database import Base


class CommunityPost(Base):
    __tablename__ = "community_posts"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    entry = Column(String)
    stop_loss = Column(String)
    target = Column(String)
    commentary = Column(Text)
    image_path = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)