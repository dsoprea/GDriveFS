import logging

from threading import Lock

_logger = logging.getLogger(__name__)


class BufferSegments(object):
    """Describe a series of strings that, when concatenated, represent the 
    whole file. This is used to try and contain the amount of the data that has
    the be copied as updates are applied to the file.
    """

    __locker = Lock()

    def __init__(self, data, block_size):
        # An array of 2-tuples: (offset, string). We should allow data to be 
        # empty. Thus, we should allow a segment to be empty (useful, in 
        # general).
        self.__segments = [(0, data)]

        self.__block_size = block_size

    def __repr__(self):
        return ("<BSEGS  SEGS= (%(segs)d) BLKSIZE= (%(block_size)d)>" % 
                { 'segs': len(self.__segments), 
                  'block_size': self.__block_size })

    def dump(self):
        pprint(self.__segments)

    def __find_segment(self, offset):

        # Locate where to insert the data.
        seg_index = 0
        while seg_index < len(self.__segments):
            seg_offset = self.__segments[seg_index][0]

            # If the current segment starts after the point of insertion.        
            if seg_offset > offset:
                return (seg_index - 1)

            # If the insertion point is at the beginning of this segment.
            elif seg_offset == offset:
                return seg_index

            seg_index += 1

        # If we get here, we never ran into a segment with an offset greater 
        # that the insertion offset.
        return (seg_index - 1)

    def __split(self, seg_index, offset):
        """Split the given segment at the given offset. Offset is relative to 
        the particular segment (an offset of '0' refers to the beginning of the 
        segment). At finish, seg_index will represent the segment containing 
        the first half of the original data (and segment with index 
        (seg_index + 1) will contain the second).
        """
    
        (seg_offset, seg_data) = self.__segments[seg_index]

        first_half = seg_data[0:offset]
        firsthalf_segment = (seg_offset, first_half)
        self.__segments.insert(seg_index, firsthalf_segment)

        second_half = seg_data[offset:]
        if second_half == '':
            raise IndexError("Can not use offset (%d) to split segment (%d) "
                             "of length (%d)." % 
                             (offset, seg_index, len(seg_data)))
        
        secondhalf_segment = (seg_offset + offset, second_half)
        self.__segments[seg_index + 1] = secondhalf_segment

        return (firsthalf_segment, secondhalf_segment)

    def apply_update(self, offset, data):
        """Find the correct place to insert the data, splitting existing data 
        segments into managable fragments ("segments"), overwriting a number of 
        bytes equal in size to the incoming data. If the incoming data will
        overflow the end of the list, grow the list.
        """

        with self.__locker:
            data_len = len(data)

            if len(self.__segments) == 1 and self.__segments[0][1] == '':
                self.__segments = []
                simple_append = True
            else:
                simple_append = (offset >= self.length)

            _logger.debug("Applying update of (%d) bytes at offset (%d). "
                          "Current segment count is (%d). Total length is "
                          "(%d). APPEND= [%s]",
                          data_len, offset, len(self.__segments), self.length, 
                          simple_append)

            if not simple_append:
                seg_index = self.__find_segment(offset)

                # Split the existing segment(s) rather than doing any 
                # concatenation. Theoretically, the effort of writing to an 
                # existing file should shrink over time.

                (seg_offset, seg_data) = self.__segments[seg_index]
                seg_len = len(seg_data)
                
                # If our data is to be written into the middle of the segment, 
                # split the segment such that the unnecessary prefixing bytes are 
                # moved to a new segment preceding the current.
                if seg_offset < offset:
                    prefix_len = offset - seg_offset
                    _logger.debug("Splitting-of PREFIX of segment (%d). Prefix "
                                  "length is (%d). Segment offset is (%d) and "
                                  "length is (%d).",
                                  seg_index, prefix_len, seg_offset, 
                                  len(seg_data))

                    (_, (seg_offset, seg_data)) = self.__split(seg_index, 
                                                               prefix_len)

                    seg_len = prefix_len
                    seg_index += 1

                # Now, apply the update. Collect the number of segments that will 
                # be affected, and reduce to two (at most): the data that we're 
                # applying, and the second part of the last affected one (if 
                # applicable). If the incoming data exceeds the length of the 
                # existing data, it is a trivial consideration.

                stop_offset = offset + data_len
                seg_stop = seg_index
                while 1:
                    # Since the insertion offset must be within the given data 
                    # (otherwise it'd be an append, above), it looks like we're 
                    # inserting into the last segment.
                    if seg_stop >= len(self.__segments):
                        break
                
                    # If our offset is within the current set of data, this is not
                    # going to be an append operation.
                    if self.__segments[seg_stop][0] >= stop_offset:
                        break
                    
                    seg_stop += 1

                seg_stop -= 1

# TODO: Make sure that updates applied at the front of a segment are correct.

                _logger.debug("Replacement interval is [%d, %d]. Current "
                              "segments= (%d)",
                              seg_index, seg_stop, len(self.__segments))

                # How much of the last segment that we touch will be affected?
                (lastseg_offset, lastseg_data) = self.__segments[seg_stop] 

                lastseg_len = len(lastseg_data)
                affected_len = (offset + data_len) - lastseg_offset
                if affected_len > 0 and affected_len < lastseg_len:
                    _logger.debug("Splitting-of suffix of segment (%d). "
                                  "Suffix length is (%d). Segment offset "
                                  "is (%d) and length is (%d).",
                                  seg_stop, lastseg_len - affected_len, 
                                  lastseg_offset, lastseg_len)

                    self.__split(seg_stop, affected_len)

                # We now have a distinct range of segments to replace with the new 
                # data. We are implicitly accounting for the situation in which our
                # data is longer than the remaining number of bytes in the file.

                _logger.debug("Replacing segment(s) (%d)->(%d) with new "
                              "segment having offset (%d) and length "
                              "(%d).", 
                              seg_index, seg_stop + 1, seg_offset, len(data))

                self.__segments[seg_index:seg_stop + 1] = [(seg_offset, data)]
            else:
                self.__segments.append((offset, data))

    def read(self, offset=0, length=None):
        """A generator that returns data from the given offset in blocks no
        greater than the block size.
        """

        with self.__locker:
            _logger.debug("Reading at offset (%d) for length [%s]. Total "
                          "length is [%s].", offset, length, self.length)

            if length is None:
                length = self.length

            current_segindex = self.__find_segment(offset)
            current_offset = offset

            boundary_offset = offset + length

            # The WHILE condition should only catch if the given length exceeds 
            # the actual length. Else, the BREAK should always be sufficient.
            last_segindex = None
            (seg_offset, seg_data, seg_len) = (None, None, None)
            while current_segindex < len(self.__segments):
                if current_segindex != last_segindex:
                    (seg_offset, seg_data) = self.__segments[current_segindex]
                    seg_len = len(seg_data)
                    last_segindex = current_segindex

                grab_at = current_offset - seg_offset
                remaining_bytes = boundary_offset - current_offset

                # Determine how many bytes we're looking for, and how many we 
                # can get from this segment.

                grab_len = min(remaining_bytes,                         # Number of remaining, requested bytes.
                               seg_len - (current_offset - seg_offset), # Number of available bytes in segment.
                               self.__block_size)                       # Maximum block size.

                grabbed = seg_data[grab_at:grab_at + grab_len]
                current_offset += grab_len
                yield grabbed

                # current_offset should never exceed boundary_offset.
                if current_offset >= boundary_offset:
                    break

                # Are we going to have to read from the next segment, next 
                # time?
                if current_offset >= (seg_offset + seg_len):
                    current_segindex += 1

    @property
    def length(self):
        if not self.__segments:
            return 0

        last_segment = self.__segments[-1]
        return last_segment[0] + len(last_segment[1])

