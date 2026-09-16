import logging

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


class ModelCreationMonitor:
    _stats = {}

    @classmethod
    def record(cls, model_name: str, is_created: bool):
        """Record a model instance being found or created.

        Args:
            model_name: Name of the model class
            is_created: True if instance was created, False if found
        """
        if model_name not in cls._stats:
            cls._stats[model_name] = {"found": 0, "created": 0}

        if is_created:
            cls._stats[model_name]["created"] += 1
        else:
            cls._stats[model_name]["found"] += 1

    @classmethod
    def log_stats(cls):
        """Log the accumulated statistics for all models."""
        logging.info("=== Model Creation Statistics ===")
        for model_name, stats in cls._stats.items():
            logging.info(
                f"{model_name:<30} Found: {stats['found']:>7}, "
                f"Created: {stats['created']:>7}, Total: {stats['found'] + stats['created']:>7}"
            )


def get_or_create(session: Session, model, unique_key: dict, **kwargs):
    """Get an existing instance or create a new one.

    Args:
        session: SQLAlchemy session
        model: The model class to query
        unique_key: Dict containing the unique identifier(s) to search by
        **kwargs: Additional fields to use when creating a new instance

    Returns:
        True if a new instance was creates OR False if the instance already exists
    """
    try:
        # Prevent auto flush so that alchemy doesnt create entities that we first want to search
        with session.no_autoflush:
            instance = session.scalar(select(model).filter_by(**unique_key))
        if instance:
            session.add(instance)
            ModelCreationMonitor.record(model.__tablename__, is_created=False)
            return instance, False
        else:
            create_args = {**unique_key, **kwargs}
            instance = model(**create_args)
            session.add(instance)
            ModelCreationMonitor.record(model.__tablename__, is_created=True)
            return instance, True

    except SQLAlchemyError as e:
        logging.error(f"Database error creating {model.__tablename__}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error creating {model.__tablename__}: {str(e)}")
        raise
