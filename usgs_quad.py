from typing import List


class UsgsQuad:
    def __init__(self, quad_id):
        self.quad_id = quad_id

    @property
    def min_lat(self):
        return self.bounds[0]

    @property
    def max_lat(self):
        return self.bounds[1]
    
    @property
    def min_lon(self):
        return self.bounds[2]
    
    @property
    def max_lon(self):
        return self.bounds[3]
    
    @classmethod
    def from_lat_lon(cls, lat, lon):
        quad_id = cls._lat_lon_to_quad_id(lat, lon)
        return cls(quad_id)
    
    def get_quad_id(self):
        return self.quad_id
    
    @property
    def bounds(self):
        lat_deg, lon_deg, lat_div, lon_div = self._parse_quad_id(self.quad_id)
        lon_deg = -1 * lon_deg
        lon_div = 1 - lon_div
        min_lat = lat_deg + (lat_div * 7.5 / 60)
        max_lat = min_lat + 7.5 / 60
        min_lon = lon_deg + ((lon_div - 1) * 7.5 / 60)
        max_lon = min_lon + 7.5 / 60
        return min_lat, max_lat, min_lon, max_lon
    
    def contains(self, lat, lon):
        min_lat, max_lat, min_lon, max_lon = self.bounds
        return min_lat <= lat < max_lat and min_lon <= lon < max_lon
    
    @staticmethod
    def _lat_lon_to_quad_id(lat, lon):
        lat_deg = int(lat)
        lon_deg = int(abs(lon))
        lat_remainder = (lat - lat_deg) * 60
        lon_remainder = (abs(lon) - lon_deg) * 60
        lat_div = int(lat_remainder / 7.5)
        lat_letter = chr(lat_div + 65)
        lon_div = int(lon_remainder / 7.5) + 1
        quad_id = f"{abs(lat_deg):02d}{lon_deg:03d}-{lat_letter}{lon_div}"
        return quad_id
    
    @staticmethod
    def _parse_quad_id(quad_id):
        lat_deg = int(quad_id[:2])
        lon_deg = int(quad_id[2:5])
        lat_div = ord(quad_id[6]) - 65
        lon_div = int(quad_id[7])
        return lat_deg, lon_deg, lat_div, lon_div
    
    def __repr__(self) -> str:
        return f"<UsgsQuad({self.quad_id})>"


def find_intersecting_quads(lower_left_lat, lower_left_lon,
                            upper_right_lat, upper_right_lon) -> List[UsgsQuad]:
    quads = []
    # Iterate over the range of latitudes and longitudes in 7.5-minute steps
    current_lat = lower_left_lat
    while current_lat < upper_right_lat:
        current_lon = lower_left_lon
        while current_lon < upper_right_lon:
            print(current_lat, current_lon)
            quad = UsgsQuad.from_lat_lon(current_lat, current_lon)
            print(quad, quad.quad_id)
            min_lat, max_lat, min_lon, max_lon = quad.get_bounds()
            print(min_lat, max_lat, min_lon, max_lon)
            # Check if the quad intersects with the given extent
            # Favor the bottom and left edges
            if min_lat <= current_lat < max_lat and min_lon < current_lon <= max_lon:
                quads.append(quad)
            current_lon += 7.5 / 60
            print('\n')
        current_lat += 7.5 / 60
    
    return quads
