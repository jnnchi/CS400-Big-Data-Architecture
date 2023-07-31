import multiprocessing as mp
import os
import random
import time


# SORT FUNCTION
def sort(arr):
    n = len(arr)
    swapped = False
    for i in range(n - 1):
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
        if not swapped:
            return


# MERGE SUBLISTS
def merge(key1, key2):
    ikey1 = 0
    ikey2 = 0
    a_res = []  # result array, DO NOT DO

    x = 0  # arbitrary counter for loop
    while ikey1 < len(key1) and ikey2 < len(key2):
        # print("x: {} -> ikeys: {} {}".format(x,ikey1,ikey2))

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

    # print(a_res)  # show result
    return a_res


# WORKER TO SORT SUBLISTS
def worker(inq, outq):
    while True:
        sublist = inq.get()
        if sublist is None:
            break
        sort(sublist)

        outq.put(sublist)
        print("{} received {}".format('sublist', os.getpid()))


if __name__ == '__main__':
    s1 = time.time()
    # CREATE QUEUES
    inq = mp.Queue()
    outq = mp.Queue()

    # CREATE BIG ARRAY
    array = [random.randint(0, 500000) for i in range(1000000)]

    # SPLIT INTO FOUR ARRAYS (stored in 2d array)
    ISET = 100
    NUM_PROCESSES = 5
    # 20 takes 152 for 100k
    # 40 takes 76 for 100k
    # 80 takes 41 secs for 100k
    # 80 takes 114 for 1mil
    # 100 takes 96 for 1mil
    i = len(array) // ISET
    sublists = []
    for a in range(ISET):
        if a == (ISET - 1):
            sublists.append(array[(ISET - 1) * i:])
        else:
            sublists.append(array[a * i:(1 + a) * i])

    # START WORKER PROCESSES
    processes = [mp.Process(target=worker, args=(inq, outq)) for i in range(NUM_PROCESSES)]
    for process in processes:
        process.start()

    # ADD EACH SUBLIST TO THE INPUT QUEUE
    for sublist in sublists:
        inq.put(sublist)

    # PUT NONE VALUES TO SIGNAL DONE
    for i in range(NUM_PROCESSES):
        inq.put(None)

    # GET SORTED SUBLISTS FROM OUTPUT QUEUE
    sorted_sublists = []
    for i in range(len(sublists)):
        sorted_sublists.append(outq.get())

    # WAIT FOR ALL WORKER PROCESSES TO FINISH
    for process in processes:
        process.join()

    # START TIMER
    start_time = time.time()

    # MERGE SORTED SUBLISTS INTO ONE
    """
    i = 3
    merges1 = []
    while i in range(ISET):
        merges1.append(merge(merge(sorted_sublists[i-3], sorted_sublists[i-2]), merge(sorted_sublists[i-3], sorted_sublists[i])))
        i+=4

    j = 1
    merges2 = []
    while j in range(len(merges1)):
        if len(merges1) % 2 == 0:
            merges2.append(merge(merges1[j-1], merges1[j]))
        else:
            if j != len(merges1) -1:
                merges2.append(merge(merges1[j - 1], merges1[j]))
            else:
                merges2.append(merges2[j])
        j+=2
    """
    # :((((
    key1 = merge(merge(sorted_sublists[0], sorted_sublists[1]), merge(sorted_sublists[2], sorted_sublists[3]))
    key2 = merge(merge(sorted_sublists[4], sorted_sublists[5]), merge(sorted_sublists[6], sorted_sublists[7]))
    key3 = merge(merge(sorted_sublists[8], sorted_sublists[9]), merge(sorted_sublists[10], sorted_sublists[11]))
    key4 = merge(merge(sorted_sublists[12], sorted_sublists[13]), merge(sorted_sublists[14], sorted_sublists[15]))
    key5 = merge(merge(sorted_sublists[16], sorted_sublists[17]), merge(sorted_sublists[18], sorted_sublists[19]))
    key6 = merge(merge(sorted_sublists[20], sorted_sublists[21]), merge(sorted_sublists[22], sorted_sublists[23]))
    key7 = merge(merge(sorted_sublists[24], sorted_sublists[25]), merge(sorted_sublists[26], sorted_sublists[27]))
    key8 = merge(merge(sorted_sublists[28], sorted_sublists[29]), merge(sorted_sublists[30], sorted_sublists[31]))
    key9 = merge(merge(sorted_sublists[32], sorted_sublists[33]), merge(sorted_sublists[34], sorted_sublists[35]))
    key10 = merge(merge(sorted_sublists[36], sorted_sublists[37]), merge(sorted_sublists[38], sorted_sublists[39]))
    key11 = merge(merge(sorted_sublists[40], sorted_sublists[41]), merge(sorted_sublists[42], sorted_sublists[43]))
    key12 = merge(merge(sorted_sublists[44], sorted_sublists[45]), merge(sorted_sublists[46], sorted_sublists[47]))
    key13 = merge(merge(sorted_sublists[48], sorted_sublists[49]), merge(sorted_sublists[50], sorted_sublists[51]))
    key14 = merge(merge(sorted_sublists[52], sorted_sublists[53]), merge(sorted_sublists[54], sorted_sublists[55]))
    key15 = merge(merge(sorted_sublists[56], sorted_sublists[57]), merge(sorted_sublists[58], sorted_sublists[59]))
    key16 = merge(merge(sorted_sublists[60], sorted_sublists[61]), merge(sorted_sublists[62], sorted_sublists[63]))
    key17 = merge(merge(sorted_sublists[64], sorted_sublists[65]), merge(sorted_sublists[66], sorted_sublists[67]))
    key18 = merge(merge(sorted_sublists[68], sorted_sublists[69]), merge(sorted_sublists[70], sorted_sublists[71]))
    key19 = merge(merge(sorted_sublists[72], sorted_sublists[73]), merge(sorted_sublists[74], sorted_sublists[75]))
    key20 = merge(merge(sorted_sublists[76], sorted_sublists[77]), merge(sorted_sublists[78], sorted_sublists[79]))
    key21 = merge(merge(sorted_sublists[80], sorted_sublists[81]), merge(sorted_sublists[82], sorted_sublists[83]))
    key22 = merge(merge(sorted_sublists[84], sorted_sublists[85]), merge(sorted_sublists[86], sorted_sublists[87]))
    key23 = merge(merge(sorted_sublists[88], sorted_sublists[89]), merge(sorted_sublists[90], sorted_sublists[91]))
    key24 = merge(merge(sorted_sublists[92], sorted_sublists[93]), merge(sorted_sublists[94], sorted_sublists[95]))
    key25 = merge(merge(sorted_sublists[96], sorted_sublists[97]), merge(sorted_sublists[98], sorted_sublists[99]))
    keys12 = merge(merge(key1, key2), merge(key3, key4))
    keys34 = merge(merge(key5, key6), merge(key7, key8))
    keys56 = merge(merge(key9, key10), merge(key11, key12))
    keys78 = merge(merge(key13, key14), merge(key15, key16))
    keys910 = merge(merge(key17, key18), merge(key19, key20))
    keys1112 = merge(merge(merge(key21, key22), merge(key23, key24)),key25)
    final = merge(merge(merge(keys12, keys34), merge(keys56, keys78)), merge(keys910,keys1112))

    # END TIMER AND PRINT TIME
    time_taken = time.time() - start_time
    print("TIME TAKEN: " + str(time_taken))

    # CHECK IF PROPERLY SORTED
    if final == sorted(array):
        print("PASS")
    else:
        print("FAIL")

    print(time.time() - s1)
