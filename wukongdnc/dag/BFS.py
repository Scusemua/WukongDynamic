graph = {
  '5' : ['3','7'],
  '3' : ['2', '4'],
  '7' : ['8'],
  '2' : [],
  '4' : ['8'],
  '8' : []
}

class Node:
    def __init__(self,ID):
        self.partition_number = -1
        self.ID = ID
        self.parents = []
        self.children = []
        self.partition = -1

N5 = Node(5)
N3 = Node(3)
N7 = Node(7)
N2 = Node(2)
N4 = Node(4)
N8 = Node(8)
N9 = Node(9)

#N5.ID = 5
N5.children = [N3,N7]
N5.parents = []

#N3.ID = 3
N3.children = [N2,N4]
N3.parents = [N5,N9]

#N7.ID = 7
N7.children = [N8]
N7.parents = [N5]

#N2.ID = 2
N2.children = [N9]
N2.parents = [N3]

#N4.ID = 4
N4.children = [N8]
N4.parents = [N3]

#N8.ID = 8
N8.children = []
N8.parents = [N7,N4]

#N9.ID = 9
N9.children = [N3,N5]
N9.parents = [N2]

visited = [] # List for visited nodes.
queue = []     #Initialize a queue
current_partition = []
current_partition_number = 1

"""
def bfs(visited, graph, node): #function for BFS
  visited.append(node)
  queue.append(node)

  while queue:          # Creating loop to visit each node
    m = queue.pop(0) 
    print (m, end = " ") 

    for neighbour in graph[m]:
      if neighbour not in visited:
        visited.append(neighbour)
        queue.append(neighbour)
"""

def dfs_p(visited, graph, node):
    print ("dfs_p from node " + str(node.ID))
    if not len(node.parents):
        print ("dfs_p node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_p node " + str(node.ID) + " visit parents")
    for neighbour in node.parents:
        if neighbour.ID not in visited:
            print ("dfs_p visit node " + str(neighbour.ID))
            dfs_parent(visited, graph, neighbour)
        else:
            print ("dfs_p neighbor.ID " + str(neighbour.ID) + " already visited")
    if node.partition_number == -1:
        print ("dfs_p add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        print ("dfs_p do not add " + str(node.ID) + " to partition "
            + str(current_partition_number) + " since it is already in partition " 
            + str(node.partition_number))

def dfs_parent(visited, graph, node):  #function for dfs 
    # e.g. dfs(3) where bfs is visiting 3 as a child of enqueued node
    # so 3 is not visited yet
    print ("dfs_parent from node " + str(node.ID))

    if not len(node.parents):
        print ("dfs_parent node " + str(node.ID) + " has no parents")
    else:
        print ("dfs_parent node " + str(node.ID) + " visit parents")
    for neighbour in node.parents:
        if neighbour.ID not in visited:
            print ("dfs_parent visit node " + str(neighbour.ID))
            dfs_parent(visited, graph, neighbour)
        else:
            print ("dfs_parent neighbor " + str(neighbour.ID) + " already visited")
    if len(node.children) == 0:
        # Can a child be visited? If it was visted then parent 9 must have been
        # already visited? since can;t visit/add to partition unless all parents in there?
        visited.append(node.ID)
        print ("dfs_parent add " + str(node.ID) + " to visited since no children")
    else:
        unvisited_children = False
        for neighbour in node.children:
            if neighbour.ID not in visited:
                print ("dfs_parent child " + str(neighbour.ID) + " not in visited")
                unvisited_children = True
                break
        if not unvisited_children:
            print ("dfs_parent add " + str(node.ID) + " to visited since it has no unvisited children")
            visited.append(node.ID)
        else:
            print ("dfs_parent do not add " + str(node.ID) + " to visited since it has unvisted children")
    
    if node.partition_number == -1:
        print ("dfs_parent add " + str(node.ID) + " to partition")
        node.partition_number = current_partition_number
        current_partition.append(node.ID)
    else:
        print ("dfs_parent do not add " + str(node.ID) + " to partition "
            + current_partition_number + " since it is already in partition " 
            + node.partition_number)

def bfs(visited, graph, node): #function for BFS
    print ("bfs mark " + str(node.ID) + " as visited and add to queue")
    visited.append(node.ID)
    queue.append(node)
    print ("dfs_parent add " + str(node.ID) + " to partition")
    node.partition_number = current_partition_number
    current_partition.append(node.ID)

    while queue:          # Creating loop to visit each node
        node = queue.pop(0) 
        #print (node.ID, end = " ") 
        print("bfs pop node " + str(node.ID) + " from queue") 

        if not len(node.children):
            print ("bfs node " + str(node.ID) + " has no children")
        else:
            print ("bfs node " + str(node.ID) + " visit children")
        for neighbour in node.children:
            if neighbour.ID not in visited:
                print ("bfs mark " + str(neighbour.ID) + " visited")
                visited.append(neighbour.ID)
                print ("bfs dfs_p("+ str(neighbour.ID) + ")")
                dfs_p(visited, graph, neighbour)
                print ("bfs after dfs_p, add " + str(neighbour.ID) + " to queue")
                queue.append(neighbour)

            else:
                print ("bfs node " + str(neighbour.ID) + " already visited")


# Driver Code
print("Following is the Breadth-First Search")
#bfs(visited, graph, '5')    # function calling
bfs(visited, graph, N5)    # function calling