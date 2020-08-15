



def extract_coupon(description):
    coupon = ''
    for s in range(len(description)):
        if description[s].isnumeric() or description[s] == '.':
            coupon =  coupon + description[s]
            #print(c)
        if description[s] == '%':
            break 
    try:
        float(coupon)
    except:
        coupon = coupon[1:]
        float(coupon)
    return coupon