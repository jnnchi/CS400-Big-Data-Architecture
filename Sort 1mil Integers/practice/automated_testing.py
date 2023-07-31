test_cases = [
  {
    'keys': [123,45,858,11,5585,1125,1111,222235],
    'results': [11,45,123,858,1111,1125,5585,222235]
  },
  {
    'keys': [12333,45,858,11,5585,1125,1111,222235],
    'results': [11,45,858,1111,1125,5585,12333,222235]
  },
  {
    'keys': [1,1,34, 5, 5, 1, 1, 34, 5, 19, 1],
    'results': [1,1,1,1,1,5,5,5,19,34,34]
  },
  {
    'keys': [90,1,34, 5, 5, 1, 1, 34, 5, 19, 1],
    'results': [1,1,1,1,5,5,5,19,34,34,90]
  },
  {
    'keys': [5,4,3,2,1,5,4,3,2,1],
    'results': [1,1,2,2,3,3,4,4,5,5]
  },
  {
    'keys': [1,2,3,4,5,1,2,3,4,5],
    'results': [1,1,2,2,3,3,4,4,5,5]
  },
  {
    'keys': [88,86,54,89,2, 3,4,2, 7],
    'results': [2,2,3,4,7,54,86,88,89]
  },
  {
    'keys': [1,2,3,4,5,6,7,8,9],
    'results': [1,2,3,4,5,6,7,8,9]
  },
  {
    'keys': [9,8,7,6,5,4,3,2,1],
    'results': [1,2,3,4,5,6,7,8,9]
  }
]

def sort_set(arr):
    n = len(arr)
    # optimize code, so if the array is already sorted, it doesn't need
    # to go through the entire process
    swapped = False
    # Traverse through all array elements
    for i in range(n - 1):
        # range(n) also work but outer loop will
        # repeat one time more than needed.
        # Last i elements are already in place
        for j in range(0, n - i - 1):
            # traverse the array from 0 to n-i-1
            # Swap if the element found is greater
            # than the next element
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]

        if not swapped:
            # if we haven't needed to make a single swap, we
            # can just exit the main loop.
            return

def test_join_code(keys):
  # SPLIT INTO TWO ARRAYS
  iset = 2
  i = int(len(keys)/iset)
  key1 = keys[:i]
  key2 = keys[i:]

  # SORT EACH ARRAY
  sort_set(key1)
  sort_set(key2)
  # print("sorted 1: {}\n sorted 2: {}".format(key1, key2))

  # JOINING/MERGING MAIN LOOP
  ikey1 = 0
  ikey2 = 0
  a_res = [] # result array, DO NOT DO
 
  x = 0 # arbitrary counter for loop
  while ikey1 < len(key1) and ikey2 < len(key2):
    # tracking keys
    # print("x: {} -> ikeys: {} {}".format(x,ikey1,ikey2))

    # if key1 is bigger
    if key1[ikey1] > key2[ikey2]:
       # print("c1") # tracking cases
      a_res.append(key2[ikey2])
      ikey2+=1
    # if key2 is bigger
    elif key1[ikey1] < key2[ikey2]:
      # print("c2")
      a_res.append(key1[ikey1])
      ikey1 += 1
    # if they're equal, saves time long term
    else:
      # print("c3")
      a_res.append(key1[ikey1])
      ikey1 += 1
      a_res.append(key2[ikey2])
      ikey2 += 1

    # if you've exhausted the first or second list
    if (ikey1 == len(key1)) and (ikey2 < len(key2)):
      a_res.extend(key2[ikey2:])
      # print("c4")
    elif (ikey2 == len(key2)) and (ikey1 < len(key1)):
      a_res.extend(key1[ikey1:])
      # print("c5")
    x+=1

  print(a_res) # show result
  return a_res

if __name__ == "__main__":
  cases = []
  for tests in test_cases:
    a_res = test_join_code(tests['keys'])
    if a_res == tests['results']:
      print("PASS")
      cases.append("PASS")
      print("*" * 20) # section divider
    else:
      print("FAIL")
      cases.append("FAIL")
      print("*" * 20) # section divider
  print(cases)
  # need a centralized data structure, so that each node has access to it
  # we'll use a local shared file system + USBs to plug into master
  # USBs will be preshared with complex test case
