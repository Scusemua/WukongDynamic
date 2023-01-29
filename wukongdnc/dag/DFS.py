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
        self.ID = ID
        self.parents = []
        self.children = []
        self.partition = -1

"""
N5 = Node(5)
#N5.ID = 5
N5.children = ['3','7']
N5.parents = []

N3 = Node(3)
#N3.ID = 3
N3.children = ['2','4']
N3.parents = ['5']

N7= Node(7)
#N7.ID = 7
N7.children = ['8']
N7.parents = ['5']

N2 = Node(2)
#N2.ID = 2
N2.children = []
N2.parents = ['3']

N4 = Node(4)
#N4.ID = 4
N4.children = ['8']
N4.parents = ['3']

N8 = Node(8)
#N8.ID = 8
N8.children = []
N8.parents = ['7','4']
"""
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
N9.parents = []

visited = set() # Set to keep track of visited nodes of graph.

"""
def dfs(visited, graph, node):  #function for dfs 
    if node not in visited:
        print (node)
        visited.add(node)
        for neighbour in graph[node]:
            dfs(visited, graph, neighbour)
"""

def dfs(visited, graph, node):  #function for dfs 
    #print("node.ID:" + str(node.ID))
    if node.ID not in visited:
        print (str(node.ID))
        visited.add(node.ID)
        for neighbour in node.children:
            dfs(visited, graph, neighbour)

# Driver Code
print("Following is the Depth-First Search")
#dfs(visited, graph, '5')
print("N5.ID:" + str(N5.ID))
dfs(visited, graph, N5)