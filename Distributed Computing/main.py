import os
import random
import time
import dispy


# SORT FUNCTION
def sort(array):
   def innersort(arr):
       n = len(arr)
       swapped = False
       for i in range(n - 1):
           for j in range(0, n - i - 1):
               if arr[j] > arr[j + 1]:
                   swapped = True
                   arr[j], arr[j + 1] = arr[j + 1], arr[j]
           if not swapped:
               return

   innersort(array)


# MERGE SUBLISTS
def merge(key1, key2):
   ikey1 = 0
   ikey2 = 0
   a_res = []  # result array, DO NOT DO

   x = 0  # arbitrary counter for loop
   while ikey1 < len(key1) and ikey2 < len(key2):
       # print("x: {} -> ikeys: {} {}".format(x,ikey1,ikey2))

       inside_loop = time.time()
       # if key1 is bigger
       if key1[ikey1] > key2[ikey2]:
           # print("c1") # tracking cases
           a_res.append(key2[ikey2])
           ikey2 += 1
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
       x += 1
       print("single merge {}".format((time.time() - inside_loop) * 1000))

   # print(a_res)  # show result
   return a_res


if __name__ == '__main__':
   s1 = time.time()

   # CREATE BIG ARRAY
   ARRSIZE = 10000
   array = [random.randint(0, 500000) for i in range(ARRSIZE)]

   # SPLIT INTO FOUR ARRAYS (stored in 2d array)

   ISET = 500 if ARRSIZE >= 500 else ARRSIZE
   # 500 sublists: 16 sec to sort, 14 sec to merge -> 32 sec total
   NUM_PROCESSES = 5

   i = len(array) // ISET
   sublists = []
   for a in range(ISET):
       if a == (ISET - 1):
           sublists.append(array[(ISET - 1) * i:])
       else:
           sublists.append(array[a * i:(1 + a) * i])

   # sorts sublists using dispy
   job_sublists = []
   cluster = dispy.JobCluster(sort,nodes=['192.168.0.*'],host=[‘192.168.0.1’],depends=[sort])
   for sublist in sublists:
       job = cluster.submit(sublist)
       job_sublists.append(job)

   # convert job objects to regular sublists
   sublists = []  # reset sublist array
   for job in job_sublists:
       sublists.append(job())

   # START TIMER
   start_time = time.time()

   # MERGE SORTED SUBLISTS INTO ONE
   big_sort = []
   for i in range(0, ISET, 4):
       big_sort.append(merge(merge(sublists[i], sublists[i + 1]), merge(sublists[i + 2], sublists[i + 3])))
   cluster.print_status()
   final = big_sort[0]
   for sublist in big_sort[1:]:
       final = merge(final, sublist)

   # END TIMER AND PRINT TIME
   time_taken = time.time() - start_time
   print("MERGE TIME " + str(time_taken * 1000))

   # CHECK IF PROPERLY SORTED
   if final == sorted(array):
       print("PASS")
   else:
       print("FAIL")

   print("TOTAL TIME " + str((time.time() - s1) * 1000))
