"""Preserve complete failure causes only inside encrypted execution evidence."""
import traceback

def error_record(error):
    causes=[];seen=set();current=error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        causes.append(dict(type=type(current).__name__,error=repr(current),
            code=getattr(current,'code',None),detail=getattr(current,'detail',None)))
        current=current.__cause__ or (None if current.__suppress_context__ else current.__context__)
    return dict(error=repr(error),causes=causes,
        traceback=''.join(traceback.format_exception(type(error),error,error.__traceback__)),
        cleanup_errors=getattr(error,'cleanup_errors',[]))
