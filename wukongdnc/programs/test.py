from channel import BiChannel, UniChannel

def test_bi_channel():
    from DivideandConquerFibonacci import ResultType

    b = BiChannel("Testing")
    result1 = ResultType()
    result1.value = 1 
    result1.problemID = "testBiChannel"

    print("Sending result1")
    b.send1(result1)
    result2 = None 
    try:
        print("Receiving result1")
        result2 = b.rcv1()
    except Exception as ex:
        print(str(ex))

    copy = result2.copy()
    copy.problemID = result2.problemID
    
    print("Sending copy of result2")
    b.send2(copy)
    print("Receiving copy of result2")
    result3 = b.rcv2()

    print("Result1: " + str(result1))
    print("Result3: " + str(result3))

if __name__ == "__main__":
    test_bi_channel()