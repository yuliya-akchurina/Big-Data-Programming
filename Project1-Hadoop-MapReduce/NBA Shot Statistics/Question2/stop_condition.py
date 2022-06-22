#!/usr/bin/python3

from mapper import get_centroids

#check if distance of old centroids and new centroids is less than 0.05
def check_centroids_distance(centroids, new_centroids):
    f1x = abs(centroids[0][0] - new_centroids[0][0])<0.05
    f1y = abs(centroids[0][1] - new_centroids[0][1])<0.05
    f1z = abs(centroids[0][2] - new_centroids[0][2])<0.05
    f2x = abs(centroids[1][0] - new_centroids[1][0])<0.05
    f2y = abs(centroids[1][1] - new_centroids[1][1])<0.05
    f2z = abs(centroids[1][2] - new_centroids[1][2])<0.05
    f3x = abs(centroids[2][0] - new_centroids[2][0])<0.05
    f3y = abs(centroids[2][1] - new_centroids[2][1])<0.05
    f3z = abs(centroids[2][2] - new_centroids[2][2])<0.05
    f4x = abs(centroids[3][0] - new_centroids[3][0])<0.05
    f4y = abs(centroids[3][1] - new_centroids[3][1])<0.05
    f4z = abs(centroids[3][2] - new_centroids[3][2])<0.05

    if f1x and f1y and f3z and f2x and f2y and f2z and f3x and f3y and f3z and f4x and f4y and f4z:
        print(1)
    else:
        print(0)

if __name__ == "__main__":
    centroids = get_centroids('centroids.txt')
    new_centroids = get_centroids('new_centroids.txt')
    
    check_centroids_distance(centroids, new_centroids)