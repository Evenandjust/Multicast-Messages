//
//  main.c
//  m_cast
//
//  Created by XZZ on 10/11/16.
//  Copyright © 2016 XZZ. All rights reserved.
//

#include "sp.h"

#include <sys/types.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>

#define int32u unsigned int
#define AGREED_MESS 0x00000010

#define BOOL int
#define TRUE 1
#define FALSE 0

static	char	User[80];
static  char    Spread_name[80];
static  char    Private_group[MAX_GROUP_NAME];
static  mailbox Mbox;
static	int	    Num_sent;
static  int     To_exit = 0;

#define MAX_MESSLEN     102400
#define MAX_VSSETS      10
#define MAX_MEMBERS     100

static	void	Read_message();
static	void	Usage( int argc, char *argv[] );
static  void    Print_help();
static  void	Bye();

static  int     mem_count = 0;
static  int     Recv_count = 0;
static  int     temp_process_index, temp_message_index, temp_ran_num, temp_total_num;
static  int     mess_num[12];      // Array to store the number of messages each process will send. Since there might be 12 processes(ugrad1-ugrad12) at most, just choose its size as 12.
static  int     sum_of_mess_num = 0;
BOOL            window_flag;
int     WINDOW_SIZE;
int     num_of_messages;
int     process_index;
int     number_of_processes;
int     start_index;
int     end_index;
FILE    *fd;

// Define a struct for message
struct Message{
    int process_index;
    int message_index;
    int ran_num;
    int total_num;
    char payload[1200];
};

int main( int argc, char *argv[] ){
    
    int	    ret;
    int     mver, miver, pver;
    sp_time test_timeout;
    int     m_ret;
    char    group[80];
    int     mess_len;
    
    WINDOW_SIZE = 5;
    
    strcpy(group, "group1");
    
    test_timeout.sec = 5;
    test_timeout.usec = 0;
    
    Usage(argc, argv);
    
    if (!SP_version( &mver, &miver, &pver))
    {
        printf("main: Illegal variables passed to SP_version()\n");
        Bye();
    }
    printf("Spread library version is %d.%d.%d\n", mver, miver, pver);
    
    ret = SP_connect_timeout( Spread_name, User, 0, 1, &Mbox, Private_group, test_timeout );
    if( ret != ACCEPT_SESSION )
    {
        SP_error( ret );
        Bye();
    }
    printf("User: connected to %s with private group %s\n", Spread_name, Private_group );
    
    // Wait for the user to enter 3 parameters
    printf("Enter the parameters: <num_of_messages> <process_index> <number_of_processes> \n");
    printf("(Input One by one and separate by space) \n");
    scanf("%d %d %d", &num_of_messages, &process_index, &number_of_processes);
    printf("\n");
    
    // Join the group
    ret = SP_join(Mbox, group);
    if (ret < 0) {
        SP_error(ret);
    }
    printf("Process %d joins the group. \n", process_index);
    
    // Wait for membership information, when all processes are ready, continue to send and receive messages
    while(mem_count<number_of_processes-process_index+1){
        Read_message();
    }
    
    struct Message message;
    
    for (int i=0; i<12; i++) {
        mess_num[i] = 0;
    }
    
    clock_t t1 = clock();

    time_t t;
    srand((unsigned) time(&t));
    
    // Initialize two attributes of the message
    if(num_of_messages>0){
        message.process_index = process_index;
        message.total_num = num_of_messages;
    }
    
    start_index = 0;
    end_index = start_index + WINDOW_SIZE;
    
    window_flag = TRUE;
    
    // Open a file according to process_index
    switch (process_index) {
        case 1:
            fd = fopen("1.out", "a");
            break;
            
        case 2:
            fd = fopen("2.out", "a");
            break;
            
        case 3:
            fd = fopen("3.out", "a");
            break;
            
        case 4:
            fd = fopen("4.out", "a");
            break;
            
        case 5:
            fd = fopen("5.out", "a");
            break;
            
        case 6:
            fd = fopen("6.out", "a");
            break;
            
        case 7:
            fd = fopen("7.out", "a");
            break;
            
        case 8:
            fd = fopen("8.out", "a");
            break;
            
        default:
            break;
    }

    
    for(;;){
        
        // Send WINDOW_SIZE number of data messages once a time
        mess_len = 1216;
        
        if(num_of_messages>0&&window_flag==TRUE){
            
            if(Num_sent<num_of_messages){
                
                for(int i=start_index;i<end_index;i++){
                    
                    message.message_index = i;
                    message.ran_num = rand() % 1000000;
                    Num_sent++;
                    m_ret= SP_multicast( Mbox, AGREED_MESS, group, 2, mess_len, (char*)&message );
                    
                    if( m_ret < 0 ){
                        SP_error( m_ret );
                        Bye();
                    }
                    
                }
            }
        }
        
        window_flag = FALSE;
        
        // Receive membership messages or regular messages
        Read_message();
        
        // Update the value of start_index and end_index
        if (start_index<num_of_messages-WINDOW_SIZE&&window_flag==TRUE) {
            start_index = start_index + WINDOW_SIZE;
            end_index = start_index + WINDOW_SIZE;
        }
        
        // Determine whether to terminate
        if (Recv_count>=sum_of_mess_num) {
            break;
        }

    }
    
    fclose(fd);
    
    clock_t t2 = clock();
    
    // Calculate the execution time
    printf("Execution time is %f. \n", (double)(t2-t1)/CLOCKS_PER_SEC);
    
    return( 0 );
}

static	void	Read_message(){
    
    static	char		 mess[MAX_MESSLEN];
    char		 sender[MAX_GROUP_NAME];
    char		 target_groups[MAX_MEMBERS][MAX_GROUP_NAME];
    membership_info  memb_info;
    int		 num_groups;
    int		 service_type;
    int16		 mess_type;
    int		 endian_mismatch;
    int		 ret;
    
    service_type = 0;
    
    ret = SP_receive( Mbox, &service_type, sender, 100, &num_groups, target_groups,
                     &mess_type, &endian_mismatch, sizeof(mess), mess );

    if( ret < 0 )
    {
        if ( (ret == GROUPS_TOO_SHORT) || (ret == BUFFER_TOO_SHORT) ) {
            service_type = DROP_RECV;
            printf("\n========Buffers or Groups too Short=======\n");
            ret = SP_receive( Mbox, &service_type, sender, MAX_MEMBERS, &num_groups, target_groups,
                             &mess_type, &endian_mismatch, sizeof(mess), mess );
        }
    }
    if (ret < 0 )
    {
        if( ! To_exit )
        {
            SP_error( ret );
            printf("\n============================\n");
            printf("\nBye.\n");
        }
        exit( 0 );
    }
    if( Is_regular_mess( service_type ) )
    {
        mess[ret] = 0;
        
        // Calculate number of received regular messages
        Recv_count = Recv_count + 1;
        
        struct Message recvMessage = *(struct Message *)&mess;
        
        // Store the message information
        temp_process_index = recvMessage.process_index;
        temp_message_index = recvMessage.message_index;
        temp_ran_num = recvMessage.ran_num;
        temp_total_num = recvMessage.total_num;
        
        if (temp_process_index==process_index&&temp_message_index==end_index-1) {
            window_flag = TRUE;
        }
        
        // Update the value in array mess_num[] according to the total number of messages to be sent of some process(temp_total_num)
        mess_num[temp_process_index-1] = temp_total_num;
        
        // Calculate the sum of array mess_num, which is also the total number of messages that will be sent by all processes
        sum_of_mess_num = 0;
        for (int i=0; i<12; i++) {
            sum_of_mess_num += mess_num[i];
        }
        
        // Write into different output files according to the process_index
        fprintf(fd,"%2d, %8d, %8d\n", temp_process_index, temp_message_index, temp_ran_num);
        
    }else if( Is_membership_mess( service_type ) )
    {
        ret = SP_get_memb_info( mess, service_type, &memb_info );
        if (ret < 0) {
            printf("BUG: membership message does not have valid body\n");
            SP_error( ret );
            exit( 1 );
        }
        if     ( Is_reg_memb_mess( service_type ) )
        {
            // Calculate number of received membership messages
            mem_count = mem_count + 1;
            
        }else if( Is_transition_mess(   service_type ) ) {
            printf("received TRANSITIONAL membership for group %s\n", sender );
        }else if( Is_caused_leave_mess( service_type ) ){
            printf("received membership message that left group %s\n", sender );
        }else printf("received incorrecty membership message of type 0x%x\n", service_type );
    } else if ( Is_reject_mess( service_type ) )
    {
        printf("REJECTED message from %s, of servicetype 0x%x messtype %d, (endian %d) to %d groups \n(%d bytes): %s\n",
               sender, service_type, mess_type, endian_mismatch, num_groups, ret, mess );
    }else printf("received message of unknown message type 0x%x with ret %d\n", service_type, ret);
    
    fflush(stdout);
}

static	void	Usage(int argc, char *argv[]){
    sprintf( User, "user" );
    sprintf( Spread_name, "4803");
    while( --argc > 0 )
    {
        argv++;
        
        if( !strncmp( *argv, "-u", 2 ) )
        {
            if (argc < 2) Print_help();
            strcpy( User, argv[1] );
            argc--; argv++;
        }else if( !strncmp( *argv, "-r", 2 ) )
        {
            strcpy( User, "" );
        }else if( !strncmp( *argv, "-s", 2 ) ){
            if (argc < 2) Print_help();
            strcpy( Spread_name, argv[1] );
            argc--; argv++;
        }else{
            Print_help();
        }
    }
}

static  void    Print_help(){
    printf( "Usage: spuser\n%s\n%s\n%s\n",
           "\t[-u <user name>]  : unique (in this machine) user name",
           "\t[-s <address>]    : either port or port@machine",
           "\t[-r ]    : use random user name");
    exit( 0 );
}

static  void	Bye(){
    To_exit = 1;
    
    printf("\nBye.\n");
    
    SP_disconnect( Mbox );
    
    exit( 0 );
}
