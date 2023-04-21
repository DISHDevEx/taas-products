def get_velocity_messages(data, pattern1, pattern2):
    """
    Parses out the velocity messages for validations from the data object passed in,
    based on the patterns that are given.
    """
    results = []
    landlide_msgs = [[key, val] for key, val in data.items() if pattern1 in key]

    for msg in landlide_msgs:
        k, v = msg
        status = k.replace(pattern1, pattern2)
        m = [v.replace("[", "").replace("]", ""), data.get(status)]
        results.append(m)

    return results
