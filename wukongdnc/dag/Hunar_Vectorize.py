import numpy as np
from sklearn.metrics.pairwise import euclidean_distances

lower_boundary = -1
upper_boundary = 1
n = 2 # dimension
sample_size = 2
    
def main():
    np.random.seed(9001) # set the seed to yield reproducible results

    X = np.random.uniform( low=lower_boundary, high=upper_boundary, size=(sample_size, n) )
    Y = np.random.uniform( low=lower_boundary, high=upper_boundary, size=(sample_size, n) )

    print( 'X: ', X )
    print( 'Y: ', Y )
    print()

    # https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.euclidean_distances.html
    # This is distance between each point in X and each point in Y for a total of 2x2=4 distances?
    euclidean_distances_vector_l = euclidean_distances(X,Y)
    print(euclidean_distances_vector_l)
    print()
    # This is pairwise, i.e., 1st point in X and first point in Y, 2nd point in X and 2nd point
    # in Y, for a total of 2 points?
    for element_of_x,element_of_y in zip(X,Y):
        distance = np.linalg.norm(element_of_x - element_of_y)
        print(distance)
    print()
    # compute distance between 2 points
    def E_D(x,y):
        dist = np.linalg.norm(x - y)
        return dist

    # https://stackoverflow.com/questions/65099293/numpy-broadcast-of-vectorized-function
    # This is vectorized using pairwise for a total of 2 points?
    v_ED = np.vectorize(E_D, signature="(n),(n)->()")
    # result is an "ndarray of shape (n_samples_X, n_samples_Y)"
    result = v_ED(X,Y)
    print(result)
   
if __name__=="__main__":
    main()

"""
output:
X:  [[-0.81559274  0.70130393]
 [ 0.80150024  0.18722638]]
Y:  [[ 0.69750597 -0.73399482]
 [ 0.00419197  0.53593124]]

[[2.08555753 0.83629846]
 [0.92707244 0.8702273 ]]

2.0855575320810003
0.8702273032657555

[2.08555753 0.8702273 ]
"""