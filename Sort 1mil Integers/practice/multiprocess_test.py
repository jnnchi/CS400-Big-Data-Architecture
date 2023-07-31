import multiprocessing as mp

def worker(q):
  while True:
    message = q.get()
    if message == "DONE":
      break
    print(message + " received")

if __name__ == '__main__':
  # CREATE LIST OF MESSAGES
  msgs = []
  for i in range(50):
    msgs.append("Message #: " + str(i))

  # CREATE QUEUE
  q = mp.Queue()
 
  # pool = mp.Pool(processes=5) # not using

  # CREATE LIST OF FIVE PROCESSES
  processes = []
  for i in range(5):
    p = mp.Process(target=worker, args=(q,))
    processes.append(p)
    p.start()

  # PUT EACH MESSAGE INTO QUEUE
  for msg in msgs:
    q.put(msg)

  # MARK FINISHED PROCESSES
  for i in range(5):
    q.put("DONE")

  # JOIN PROCESSES
  for p in processes:
    p.join()
