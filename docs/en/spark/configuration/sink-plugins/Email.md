# Sink plugin : Email [Spark]

## Description

Supports data output through `email attachments`. The attachments are in the `xlsx` format that supports `excel` to open, which can be used to notify the task statistics results through email attachments.

## Options

| name     | type   | required | default value |
| -------- | ------ | -------- | ------------- |
| subject  | string | yes      | -             |
| from     | string | yes      | -             |
| to       | string | yes      | -             |
| bodyText | string | no       | -             |
| bodyHtml | string | no       | -             |
| cc       | string | no       | -             |
| bcc      | string | no       | -             |
| host     | string | yes      | -             |
| port     | string | yes      | -             |
| password | string | yes      | -             |
| limit    | string | no       | 100000        |

### subject [string]

Email Subject

### from [string]

Email sender

### to [string]

Email recipients, multiple recipients separated by `,`

### bodyText [string]

Email content, text format

### bodyHtml [string]

Email content, hypertext content

### cc [string]

Email CC, multiple CCs separated by `,`

### bcc [string]

Email Bcc, multiple Bccs separated by `,`

### host [string]

Email server address, for example: `stmp.exmail.qq.com`

### port [string]

Email server port For example: `25`

### password [string]

The password of the email sender, the user name is the sender specified by `from`

### limit [string]

The number of data is limited, the default is `100000`

## Examples

```bash
Email {
	subject = "Report statistics",
	from = "xxxx@qq.com",
	to = "xxxxx1@qq.com,xxxxx2@qq.com",
    cc = "xxxxx3@qq.com,xxxxx4@qq.com",
    bcc = "xxxxx5@qq.com,xxxxx6@qq.com",
	host= "stmp.exmail.qq.com",
	port= "25",
	password = "***********",
    limit = "1000"
}
```
