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

    <title>MongoMart - Shop MongoDB Gear</title>

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
                <li class="active">${category_param}</li>
            </ol>
        </div>
    </div>
    <!-- /.row -->

    <div class="row">

        <div class="col-md-2">
            <div class="list-group">
                <#list categories as category>
                    <a href="/?category=${category.name}" class="list-group-item <#if category_param == category.name>active</#if>"><span class="badge">${category.num_items}</span>${category.name}</a>
                </#list>
            </div>
        </div>

        <div class="col-md-10">


            <#list items as item>

                <!-- Project One -->
                <div class="row">
                    <div class="col-md-7">
                        <a href="/item?id=${item.id}">
                            <img class="img-responsive" src="${item.img_url}" alt="">
                        </a>
                    </div>
                    <div class="col-md-5">
                        <h3><a href="/item?id=${item.id}">${item.title}</a></h3>
                        <h4>${item.slogan}</h4>
                        <p>${item.description}</p>
                        <a class="btn btn-primary" href="/item?id=${item.id}">View Product <span class="glyphicon glyphicon-chevron-right"></span></a>
                    </div>
                </div>
                <!-- /.row -->

                <hr>

            </#list>


            <!-- Pagination -->
            <div class="row text-center">
                <div class="col-lg-12">
                    <ul class="pagination">

                        <!-- Show page numbers for pagination -->
                        <#if useRangeBasedPagination == false>

                            <#list 0..num_pages as i>
                                <li <#if page == (i)>class="active"</#if>>
                                    <a href="/?page=${i}&category=${category_param}">${i+1}</a>
                                </li>
                            </#list>

                        </#if>

                            <!-- TODO-lab3 Range based Pagination, modify the useRangeBasedPagination attribute in the controller  -->

                        <#if useRangeBasedPagination == true>

                            <#if previousPageUrl??>
                                <li><a href="${previousPageUrl}">Previous Page</a></li>
                            </#if>
                            <#if nextPageUrl??>
                                <li><a href="${nextPageUrl}">Next Page</a></li>
                            </#if>

                        </#if>

                    </ul>
                </div>
            </div>

            <#if useRangeBasedPagination == false>

                <div style="text-align:center;">
                    <i>${item_count} Products</i>
                </div>

            </#if>

            <!-- /.row -->
        </div>



    </div>

</div>
<!-- /.container -->

<#-- Include footer -->
<#include "includes/footer.ftl">

</body>

</html>
