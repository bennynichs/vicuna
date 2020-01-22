from alpaca import *
start_time = time.time()

responses_delete = delete_all_orders()
print(responses_delete)

responses_close = close_all_positions()
print(responses_close)
print("--- %s seconds ---" % (time.time() - start_time))
