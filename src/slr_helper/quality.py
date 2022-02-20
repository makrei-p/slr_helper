from slr_helper import Util

class FrameQuality:

    def __init__(self, core_df):
        self.core_frame = core_df
        self.hit = [0] * len(core_df)

    def get_core_coverage(self, frame):
        hits = Util.find_duplicate_indices_two_frames(self.core_frame, frame, short_circuit=True)
        for i,_ in hits:
            self.hit[i] += 1
        return len(hits) / len(self.core_frame)

    def get_hit_stats(self):
        return self.hit.copy()
