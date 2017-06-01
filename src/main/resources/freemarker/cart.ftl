<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="">
    <meta name="author" content="">
    <link rel="icon" href="/img/favicon.ico" type="image/x-icon" />
    <link rel="shortcut icon" href="/img/favicon.ico" type="image/x-icon" />

    <title>Your Cart - MongoMart</title>

    <!-- Bootstrap Core CSS -->
    <link href="/css/bootstrap.min.css" rel="stylesheet">

    <!-- Custom CSS -->
    <link href="/css/shop-homepage.css" rel="stylesheet">

    <!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
    <script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
    <script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
    <![endif]-->
</head>

<body>

<#-- Include navigation -->
<#include "includes/nav.ftl">

<!-- Page Content -->
<div class="container">

    <div class="row">
        <div class="col-md-12">
            <ol class="breadcrumb">
                <li><a href="/">Home</a></li>
                <li class="active">Cart</li>
            </ol>
        </div>
    </div>

    <#if updated??>
        <p class="bg-warning" style="padding: 15px; font-size: 14px;">

            Your cart has been successfully updated.
        </p>
    </#if>

    <div class="row">
        <div class="col-md-12">




            <table class="table table-bordered table-striped">
                <thead>
                <tr>
                    <th>Product Name</th>
                    <th>Image</th>
                    <th>Quantity</th>
                    <th>Unit Price</th>
                    <th>Total</th>
                </tr>
                </thead>
                <tbody>

                <#list cart.items as item>
                <tr>
                    <form action="/cart/update">
                        <input type="hidden" name="itemid" value="${item.id}" />

                        <td><a href="/item?id=${item.id}">${item.title}</a></td>
                        <td class="muted center_text"><a href="/item?id=${item.id}"><img width="300" src="${item.img_url}"></a></td>
                        <td>
                            <select name="quantity" onchange="this.form.submit()">
                                <option value="0">0 (Remove)</option>
                                <#list 1..25 as i>
                                    <#if item.quantity == i>
                                        <option value="${i}" selected>${i}</option>
                                    <#else>
                                        <option value="${i}">${i}</option>
                                    </#if>
                                </#list>
                            </select>
                        </td>
                        <td>${item.price?string.currency}</td>
                        <td>${(item.price * item.quantity)?string.currency}</td>

                    </form>
                </tr>
                </#list>

                <tr>
                    <td>&nbsp;</td>
                    <td>&nbsp;</td>
                    <td>&nbsp;</td>
                    <td>&nbsp;</td>
                    <td><strong>${total?string.currency}</strong></td>
                </tr>
                </tbody>
            </table>

        </div>



        <div class="row">
            <div class="col-md-12" style="text-align:right; padding-right: 30px;">
                <button class="btn btn-success" type="submit">Proceed to Checkout</button>
            </div>
        </div>
    </div>



</div>
<!-- /.container -->

<#-- Include footer -->
<#include "includes/footer.ftl">

</body>

</html>
