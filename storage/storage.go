package storage

import (
	"fmt"
	"io/ioutil"
	"strings"
)

type DiskLocation struct {
	directory string
	volumes   map[VolumeId]*Volume
}

type Store struct {
	locations []*DiskLocation
    counter uint32
}

func NewStore(dirNames []string) (s *Store) {
	s = &Store{}
	s.locations = make([]*DiskLocation, 0)
	for i := 0; i < len(dirNames); i++ {
		d := &DiskLocation{directory: dirNames[i]}
		d.volumes = make(map[VolumeId]*Volume)
		d.loadExistVolumes()
		s.locations = append(s.locations, d)
	}
	return
}

func (s *Store) AddVolume(volumeList string, collection string) error {
	for _, id_string := range strings.Split(volumeList, ",") {
		id, _ := NewVolumeId(id_string)
		s.addVolume(id, collection)
	}
	return nil
}

func (s *Store) findVolume(vid VolumeId) *Volume {
	for _, location := range s.locations {
		v, found := location.volumes[vid]
		if found {
			return v
		}
	}
	return nil
}

func (s *Store) findFreeLocation() *DiskLocation {
	//fake implement
	return s.locations[0]
}

func (s *Store) addVolume(vid VolumeId, collection string) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume found")
	}
	if location := s.findFreeLocation(); location != nil {
		if volume, err := NewVolume(location.directory, collection, vid); err == nil {
			location.volumes[vid] = volume
			return nil
		}
	}
	return fmt.Errorf("no free volume found")
}

func (l *DiskLocation) loadExistVolumes() {
	if dirs, err := ioutil.ReadDir(l.directory); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
				collection := ""
				base := name[:len(name)-len(".dat")]
				i := strings.Index(base, "_")
				if i > 0 {
					collection, base = base[:i], base[i+1:]
				}
				if vid, err := NewVolumeId(base); err == nil {
					if l.volumes[vid] == nil {
						if v, e := NewVolume(l.directory, collection, vid); e == nil {
							l.volumes[vid] = v
						}
					}
				}
			}
		}
	}
}

func (s *Store) Close() {
	for _, location := range s.locations {
		for _, volume := range location.volumes {
			volume.Close()
		}
	}
}

func (s *Store) Write(vid VolumeId, n *Needle) (size uint32, err error) {
	if v := s.findVolume(vid); v != nil {
		size, err = v.Write(n)
		return
	}
	return 0, fmt.Errorf("not such volume")
}

func (s *Store) Read(vid VolumeId, n *Needle) (size int, err error) {
	if v := s.findVolume(vid); v != nil {
		size, err = v.Read(n)
		return
	}
	return 0, fmt.Errorf("not such volume")
}

func (s *Store) GetVolume(vid VolumeId) *Volume {
	return s.findVolume(vid)
}

func (s *Store) HasVolume(vid VolumeId) bool {
	v := s.findVolume(vid)
	return v != nil
}
/*
func (s *Store) FreeVolume() *Volume {
	for _, v := range s.locations[0].volumes {
		if v.Num() < 3000 {
			return v
		}
	}
	return nil
}
*/
func (s *Store) FreeVolume() *Volume {
    var vid VolumeId
    vid = (VolumeId)(s.counter % 3 + 1)
    s.counter++
    return s.locations[0].volumes[vid]
}

