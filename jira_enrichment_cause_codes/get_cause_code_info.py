def get_cause_code_info(data, pattern):
    """
    Parses out the cause code information from the data object passed in, based on the pattern that is given.
    """

    res = [val for key, val in data.items() if pattern in key]

    return res
