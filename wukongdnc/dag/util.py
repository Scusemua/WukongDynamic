def pack_data(o, d, key_types=object):
   """ Merge known data into tuple or dict

   Parameters
   ----------
   o:
      core data structures containing literals and keys
   d: dict
      mapping of keys to data

   Examples
   --------
   >>> data = {'x': 1}
   >>> pack_data(('x', 'y'), data)
   (1, 'y')
   >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
   {'a': 1, 'b': 'y'}
   >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
   {'a': [1], 'b': 'y'}
   """
   typ = type(o)
   try:
      if isinstance(o, key_types) and o in d:
         return d[o]
   except TypeError:
      pass

   if typ in (tuple, list, set, frozenset):
      return typ([pack_data(x, d, key_types=key_types) for x in o])
   elif typ is dict:
      return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
   else:
      return o

    # Example:
    # 
    # task = (func_obj, "task1", "task2", "task3")
    # func = task[0]
    # args = task[1:] # everything but the 0'th element, ("task_id1", "taskid2", "taskid3")

    # # Intermediate data; from executing other tasks.
    # # task IDs and their outputs
    # data_dict = {
    #     "task1": 1, 
    #     "task2": 10,
    #     "task3": 3
    # }

    # args2 = pack_data(args, data_dict) # (1, 10, 3)

    # func(*args2)