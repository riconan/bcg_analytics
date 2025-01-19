import importlib

__all__ = ['get_analytics_function']

def get_analytics_function(analytics_id : str):

    master_analytics_module_name = 'analytics'
    local_analytics_module = importlib.import_module('.'.join([master_analytics_module_name, analytics_id]))
    analytics_function = getattr(local_analytics_module, analytics_id)

    print(analytics_function)
    return analytics_function


