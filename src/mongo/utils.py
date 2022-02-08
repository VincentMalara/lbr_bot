def clean_list_dict_nan(x):
    if isinstance(x, dict):
        y = {k: x[k] for k in x if not x[k] == ''}
    elif isinstance(x, list):
        y=[]
        for dict_ in x:
            if isinstance(dict_, dict):
                clean_dict = {k: dict_[k] for k in dict_ if not dict_[k] == ''}
                y.append(clean_dict)
            else:
                y.append({})
    return y
