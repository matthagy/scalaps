# The original scala example from https://stackoverflow.com/questions/49001986/left-to-right-application-of-operations-on-a-list-in-python3
# val a = ((1 to 50)
#   .map(_ * 4)
#   .filter( _ <= 170)
#   .filter(_.toString.length == 2)
#   .filter (_ % 20 == 0)
#   .zipWithIndex
#   .map{ case(x,n) => s"Result[$n]=$x"}
#   .mkString("  .. "))

from scalaps import *
a = (ScSeq(range(1,51))
     .map(lambda x: x * 4)
     .filter(lambda x: x <= 170)
     .filter(lambda x: len(str(x)) == 2)
     .filter( lambda x: x % 20 ==0)
     .enumerate()
     .map(lambda x: 'Result[%d]=%s' %(x[0],x[1]))
     .mkstring(' .. '))
print(a)

  