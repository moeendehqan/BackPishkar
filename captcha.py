import cv2  
import numpy as np
import random

def rotateImage(image, angle):
    row,col = 100,250
    center=tuple(np.array([row,col])/2)
    rot_mat = cv2.getRotationMatrix2D(center,angle,1.0)
    new_image = cv2.warpAffine(image, rot_mat, (col,row))
    return new_image

def captchaGenerate():
    font = cv2.FONT_HERSHEY_COMPLEX
    captcha = np.zeros((100,250,3), np.uint8)
    captcha[:] = (random.randint(235,255),random.randint(245,255),random.randint(245,255))
    font= cv2.FONT_HERSHEY_SIMPLEX
    texcode = ''
    for i in range(1,5):
        bottomLeftCornerOfText = (40*i,60+(random.randint(-20,20)))
        fontScale= 1.5
        fontColor= (random.randint(0,180),random.randint(0,180),random.randint(0,180))
        thickness= 3
        lineType= 1
        text = str(random.randint(0,9))
        texcode = texcode+(text)
        cv2.putText(captcha,text,bottomLeftCornerOfText,font,fontScale,fontColor,thickness,lineType)
        captcha = rotateImage(captcha,random.randint(-3,3))
    address = f'C:\\Users\\moeen\\Desktop\\project\\pishkar\\Front\\pishkar\\public\\captcha\\'+texcode+'.jpg'
    status = cv2.imwrite(address,captcha)
    return texcode

