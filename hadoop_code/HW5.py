
def createIndex(shapefile):# target geojson
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(po, pd, index, zones):
    OriginPoint = index.intersection((po.x, po.y, po.x, po.y))
    DestinPoint = index.intersection((pd.x, pd.y, pd.x, pd.y))
    for idxO in OriginPoint:
        for idxD in DestinPoint:
            if zones.geometry[idxO].contains(po) and zones.geometry[idxD].contains(pd):
                return((zones.borough[idxO], zones.neighborhood[idxO],zones.borough[idxD]))

def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex('dataset_mh5172/neighborhoods.geojson')

    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}

    for row in reader:
        pdt = row[0].split(' ')[1].split(':')[0]
        if pdt!='10':
            continue
        po = geom.Point(proj(float(row[3]), float(row[2])))
        pd = geom.Point(proj(float(row[5]), float(row[4])))
        zone = findZone(po,pd,index, zones)
        if zone:
            counts[zone] = counts.get(zone, 0) + 1
    return counts.items()
def sortvalue(aList):
    import numpy as np
    value = []
    for i in aList:
        value.append(i[1])
    index = np.array(value).argsort()[-3:][::-1]
    top = []
    for i in index:
        top.append(aList[i])
    return(top)


if __name__ == "__main__":
    import gzip
    import shutil
    with gzip.open('yellow_tripdata_2011-05.csv.gz', 'rb') as f_in:
        with open('yellow_tripdata_2011-05.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    sc = SparkContext()
    rdd = sc.textFile('dataset_mh5172/green.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips).map(lambda x:((x[0][0],x[0][2]),(x[0][1],int(x[1])))).groupByKey().mapValues(list).mapValues(sortvalue)            .collect()
    print(counts)

