from common.database.postgres.create_db_session import create_db_session


class BatchRequester:

    def __init__(self, model, batch_size=64):
        self.model = model
        self.batch_size = batch_size
        self.session_factory = create_db_session()

    def next_batch(self, offset_start: int = 0):
        with self.session_factory() as session:
            offset = offset_start
            while True:
                batch = (
                    session.query(self.model.id, self.model.abstract)
                    .filter(
                        self.model.abstract.isnot(None)
                        # self.model.source_system != 'cordis'
                    )
                    .order_by(self.model.id)
                    .offset(offset)
                    .limit(self.batch_size)
                    .all()
                )

                if not batch:
                    break

                yield batch
                offset += self.batch_size

# class BatchRequester:
#     """
#     Generic batch requester that can handle different models and field selections.
#     Supports RO and Institution patterns.
#     """
#
#     def __init__(self, model, batch_size=64, filter_condition=None):
#         self.model = model
#         self.batch_size = batch_size
#         self.filter_condition = filter_condition
#         self.session_factory = create_db_session()
#
#     def next_ro_batch(
#         self, offset_start: int = 0
#     ) -> Generator[List[Tuple[int, str]], None, None]:
#         with self.session_factory() as session:
#             offset = offset_start
#             while True:
#                 query = (
#                     session.query(self.model.id, self.model.full_text)
#                     .filter(self.model.full_text.isnot(None))
#                     .order_by(self.model.id)
#                     .offset(offset)
#                     .limit(self.batch_size)
#                 )
#
#                 if self.filter_condition is not None:
#                     query = query.filter(self.filter_condition)
#
#                 batch = query.all()
#
#                 if not batch:
#                     break
#
#                 yield batch
#                 offset += self.batch_size
#
#     def next_institution_batch(
#         self, offset_start: int = 0
#     ) -> Generator[List[Institutions], None, None]:
#         with self.session_factory() as session:
#             offset = offset_start
#             while True:
#                 query = (
#                     session.query(self.model)
#                     .order_by(self.model.id)
#                     .offset(offset)
#                     .limit(self.batch_size)
#                 )
#
#                 if self.filter_condition is not None:
#                     query = query.filter(self.filter_condition)
#
#                 batch = query.all()
#
#                 if not batch:
#                     break
#
#                 yield batch
#                 offset += self.batch_size
#
#     def get_total_count(self) -> int:
#         """Get total number of records that match the filter condition."""
#         with self.session_factory() as session:
#             query = session.query(func.count(self.model.id))
#
#             if self.filter_condition is not None:
#                 query = query.filter(self.filter_condition)
#
#             return query.scalar()



