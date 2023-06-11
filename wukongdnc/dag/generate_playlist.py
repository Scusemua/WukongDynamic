

# 6:25pm I Tcho Tchass by Akofa Akoussah on Togo Soul 70

def main(name):
    complete_name = './'+name+'.txt'
    with open(complete_name) as f:
        for line in f.readlines():
            #line = "6:25pm I Tcho Tchass by Akofa Akoussah on Togo Soul 70"
            first_blank = line.index(" ")
            num_bys = line.count(" by ")
            if num_bys > 1:
                print("")
                print("*******Warning********: more than one 'by' in line "
                + line)
            by = line.index("by ")
            after_by = line[by:]
            #print("after_by: " + after_by)
            num_ons = after_by.count(" on ")
            if num_ons > 1:
                print("")
                print("*******Warning********: more than one 'on' (afer by, so in artist) in line "
                + line)
            on = after_by.index(" on ")
            #print("by:" + str(by) + " on:" + str(on))
            song = line[(first_blank+1):by]
            #print("song: " + song)
            artist = after_by[3:on]
            #print("artist: " + artist)
            print(str(artist) + ", " + str(song))

if __name__=="__main__":
    main("playlist5")